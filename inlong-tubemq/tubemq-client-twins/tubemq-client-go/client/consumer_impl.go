// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package client

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/config"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/errs"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/log"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/metadata"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/multiplexing"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/protocol"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/remote"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/rpc"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/selector"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/sub"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/transport"
	"github.com/apache/inlong/inlong-tubemq/tubemq-client-twins/tubemq-client-go/util"
)

const (
	consumeStatusNormal        = 0
	consumeStatusFromMax       = 1
	consumeStatusFromMaxAlways = 2
	msgFlagIncProperties       = 0x01
)

type consumer struct {
	clientID         string
	config           *config.Config
	subInfo          *sub.SubInfo
	rmtDataCache     *remote.RmtDataCache
	visitToken       int64
	authorizedInfo   string
	nextAuth2Master  int32
	nextAuth2Broker  int32
	master           *selector.Node
	client           rpc.RPCClient
	selector         selector.Selector
	lastMasterHb     int64
	masterHBRetry    int
	heartbeatManager *heartbeatManager
	unreportedTimes  int
	cancel           context.CancelFunc
	routineClosed    chan struct{}
	closeOnce        sync.Once
}

// NewConsumer returns a consumer which is constructed by a given config.
func NewConsumer(config *config.Config) (Consumer, error) {
	if err := config.ValidateConsumer(); err != nil {
		return nil, err
	}
	log.Infof("The config of the consumer is %s", config)
	selector, err := selector.Get("ip")
	if err != nil {
		return nil, err
	}

	clientID := newClient(config.Consumer.Group)
	pool := multiplexing.NewPool()
	opts := &transport.Options{}
	if config.Net.TLS.Enable {
		opts.TLSEnable = true
		opts.CACertFile = config.Net.TLS.CACertFile
		opts.TLSCertFile = config.Net.TLS.TLSCertFile
		opts.TLSKeyFile = config.Net.TLS.TLSKeyFile
		opts.TLSServerName = config.Net.TLS.TLSServerName
	}
	client := rpc.New(pool, opts, config)
	r := remote.NewRmtDataCache()
	r.SetConsumerInfo(clientID, config.Consumer.Group)
	c := &consumer{
		config:          config,
		clientID:        clientID,
		subInfo:         sub.NewSubInfo(config),
		rmtDataCache:    r,
		selector:        selector,
		client:          client,
		visitToken:      util.InvalidValue,
		unreportedTimes: 0,
		routineClosed:   make(chan struct{}),
	}

	ctx := context.Background()
	ctx, c.cancel = context.WithCancel(ctx)

	c.subInfo.SetClientID(clientID)
	hbm := newHBManager(c)
	c.heartbeatManager = hbm

	go c.routine(ctx)
	go c.processRebalanceEvent(ctx)

	log.Infof("[CONSUMER] start consumer success, client=%s", clientID)
	return c, nil
}

func (c *consumer) routine(ctx context.Context) {
	defer close(c.routineClosed)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// 选主
		node, err := c.selector.Select(c.config.Consumer.Masters)
		if err != nil {
			log.Errorf("[CONSUMER] select error %v", err)
			time.Sleep(time.Second)
			continue
		}
		c.master = node
		log.Infof("[CONSUMER] master %+v", c.master)
		// 注册
		if err := c.register2Master(ctx); err != nil {
			log.Errorf("[CONSUMER] register2Master error %v", err)
			time.Sleep(time.Second)
			continue
		}
		c.lastMasterHb = time.Now().UnixMilli()
		// 心跳
		time.Sleep(c.config.Heartbeat.Interval / 2)
		if err := c.heartbeat2Master(ctx); err != nil {
			log.Errorf("[CONSUMER] heartbeat2Master error %v", err)
		} else {
			c.lastMasterHb = time.Now().UnixMilli()
		}
		heartbeatRetry := 0
		for {
			time.Sleep(c.config.Heartbeat.Interval)
			select {
			case <-ctx.Done():
				return
			default:
			}
			if heartbeatRetry >= c.config.Heartbeat.MaxRetryTimes {
				break
			}
			if err := c.heartbeat2Master(ctx); err != nil {
				log.Errorf("[CONSUMER] heartbeat2Master error %v", err)
				heartbeatRetry++
				continue
			} else {
				heartbeatRetry = 0
				c.lastMasterHb = time.Now().UnixMilli()
			}
		}
	}
}

func (c *consumer) register2Master(ctx context.Context) error {
	rsp, err := c.sendRegRequest2Master(ctx)
	if err != nil {
		return err
	}
	if !rsp.GetSuccess() {
		return errs.New(rsp.GetErrCode(), rsp.GetErrMsg())
	}
	c.processRegisterResponseM2C(rsp)
	return nil
}

func (c *consumer) sendRegRequest2Master(ctx context.Context) (*protocol.RegisterResponseM2C, error) {
	ctx, cancel := context.WithTimeout(ctx, c.config.Net.ReadTimeout)
	defer cancel()

	m := &metadata.Metadata{}
	node := &metadata.Node{}
	node.SetHost(util.GetLocalHost())
	node.SetAddress(c.master.Address)
	auth := &protocol.AuthenticateInfo{}
	if c.needGenMasterCertificateInfo(true) {
		util.GenMasterAuthenticateToken(auth, c.config.Net.Auth.UserName, c.config.Net.Auth.Password)
	}
	m.SetNode(node)
	sub := &metadata.SubscribeInfo{}
	sub.SetGroup(c.config.Consumer.Group)
	m.SetSubscribeInfo(sub)

	rsp, err := c.client.RegisterRequestC2M(ctx, m, c.subInfo, c.rmtDataCache)
	return rsp, err
}

func (c *consumer) processRegisterResponseM2C(rsp *protocol.RegisterResponseM2C) {
	if !rsp.GetNotAllocated() {
		c.subInfo.CASIsNotAllocated(1, 0)
	}
	if rsp.GetDefFlowCheckId() != 0 || rsp.GetGroupFlowCheckId() != 0 {
		if rsp.GetDefFlowCheckId() != 0 {
			c.rmtDataCache.UpdateDefFlowCtrlInfo(rsp.GetDefFlowCheckId(), rsp.GetDefFlowControlInfo())
		}
		qryPriorityID := c.rmtDataCache.GetQryPriorityID()
		if rsp.GetQryPriorityId() != 0 {
			qryPriorityID = rsp.GetQryPriorityId()
		}
		c.rmtDataCache.UpdateGroupFlowCtrlInfo(qryPriorityID, rsp.GetGroupFlowCheckId(), rsp.GetGroupFlowControlInfo())
	}
	if rsp.GetAuthorizedInfo() != nil {
		c.processAuthorizedToken(rsp.GetAuthorizedInfo())
	}
}

func (c *consumer) processAuthorizedToken(info *protocol.MasterAuthorizedInfo) {
	atomic.StoreInt64(&c.visitToken, info.GetVisitAuthorizedToken())
	c.authorizedInfo = info.GetAuthAuthorizedToken()
}

func (c *consumer) heartbeat2Master(ctx context.Context) error {
	rsp, err := c.sendHeartbeat2Master(ctx)
	if err != nil {
		return err
	}
	if !rsp.GetSuccess() {
		return errs.New(rsp.GetErrCode(), rsp.GetErrMsg())
	}
	c.processHBResponseM2C(rsp)
	return nil
}

func (c *consumer) sendHeartbeat2Master(ctx context.Context) (*protocol.HeartResponseM2C, error) {
	if time.Now().UnixNano()/int64(time.Millisecond)-c.lastMasterHb > 30000 {
		c.rmtDataCache.HandleExpiredPartitions(c.config.Consumer.MaxConfirmWait)
	}
	m := &metadata.Metadata{}
	node := &metadata.Node{}
	node.SetHost(util.GetLocalHost())
	node.SetAddress(c.master.Address)
	m.SetNode(node)
	sub := &metadata.SubscribeInfo{}
	sub.SetGroup(c.config.Consumer.Group)
	m.SetSubscribeInfo(sub)
	auth := &protocol.AuthenticateInfo{}
	if c.needGenMasterCertificateInfo(true) {
		util.GenMasterAuthenticateToken(auth, c.config.Net.Auth.UserName, c.config.Net.Auth.Password)
	}
	c.unreportedTimes++
	if c.unreportedTimes > c.config.Consumer.MaxSubInfoReportInterval {
		m.SetReportTimes(true)
		c.unreportedTimes = 0
	}

	ctx, cancel := context.WithTimeout(ctx, c.config.Net.ReadTimeout)
	defer cancel()
	rsp, err := c.client.HeartRequestC2M(ctx, m, c.subInfo, c.rmtDataCache)
	return rsp, err
}

func (c *consumer) processHBResponseM2C(rsp *protocol.HeartResponseM2C) {
	c.masterHBRetry = 0
	if !rsp.GetNotAllocated() {
		c.subInfo.CASIsNotAllocated(1, 0)
	}
	if rsp.GetDefFlowCheckId() != 0 || rsp.GetGroupFlowCheckId() != 0 {
		if rsp.GetDefFlowCheckId() != 0 {
			c.rmtDataCache.UpdateDefFlowCtrlInfo(rsp.GetDefFlowCheckId(), rsp.GetDefFlowControlInfo())
		}
		qryPriorityID := c.rmtDataCache.GetQryPriorityID()
		if rsp.GetQryPriorityId() != 0 {
			qryPriorityID = rsp.GetQryPriorityId()
		}
		c.rmtDataCache.UpdateGroupFlowCtrlInfo(qryPriorityID, rsp.GetGroupFlowCheckId(), rsp.GetGroupFlowControlInfo())
	}
	if rsp.GetAuthorizedInfo() != nil {
		c.processAuthorizedToken(rsp.GetAuthorizedInfo())
	}
	if rsp.GetRequireAuth() {
		atomic.StoreInt32(&c.nextAuth2Master, 1)
	}
	if rsp.GetEvent() != nil {
		event := rsp.GetEvent()
		subscribeInfo := make([]*metadata.SubscribeInfo, 0, len(event.GetSubscribeInfo()))
		for _, sub := range event.GetSubscribeInfo() {
			s, err := metadata.NewSubscribeInfo(sub)
			if err != nil {
				continue
			}
			subscribeInfo = append(subscribeInfo, s)
		}
		e := metadata.NewEvent(event.GetRebalanceId(), event.GetOpType(), subscribeInfo)
		c.rmtDataCache.OfferEvent(e)
	}
}

// GetMessage implementation of TubeMQ consumer.
func (c *consumer) GetMessage() (*ConsumerResult, error) {
	partition, bookedTime, err := c.rmtDataCache.SelectPartition()
	if err != nil {
		return nil, err
	}
	confirmContext := partition.GetPartitionKey() + "@" + strconv.FormatInt(bookedTime, 10)
	isFiltered := c.subInfo.IsFiltered(partition.GetTopic())
	pi := &PeerInfo{
		BrokerHost:   partition.GetBroker().GetHost(),
		PartitionID:  uint32(partition.GetPartitionID()),
		PartitionKey: partition.GetPartitionKey(),
		CurrOffset:   util.InvalidValue,
	}
	m := &metadata.Metadata{}
	node := &metadata.Node{}
	node.SetHost(util.GetLocalHost())
	node.SetAddress(partition.GetBroker().GetAddress())
	m.SetNode(node)
	sub := &metadata.SubscribeInfo{}
	sub.SetGroup(c.config.Consumer.Group)
	sub.SetPartition(partition)
	m.SetSubscribeInfo(sub)

	ctx, cancel := context.WithTimeout(context.Background(), c.config.Net.ReadTimeout)
	defer cancel()
	rsp, err := c.client.GetMessageRequestC2B(ctx, m, c.subInfo, c.rmtDataCache)
	if err != nil {
		log.Infof("[CONSUMER]GetMessage error %s", err.Error())
		if err := c.rmtDataCache.ReleasePartition(true,
			isFiltered, confirmContext, false); err != nil {
			log.Errorf("[CONSUMER]GetMessage release partition error %s", err.Error())
			return nil, err
		}
		return nil, err
	}
	cr := &ConsumerResult{
		TopicName:      partition.GetTopic(),
		ConfirmContext: confirmContext,
		PeerInfo:       pi,
	}
	msgs, err := c.processGetMessageRspB2C(pi, isFiltered, partition, confirmContext, rsp)
	if err != nil {
		return cr, err
	}
	cr.Messages = msgs
	return cr, err
}

// Confirm implementation of TubeMQ consumer.
func (c *consumer) Confirm(confirmContext string, consumed bool) (*ConfirmResult, error) {
	partitionKey, bookedTime, err := util.ParseConfirmContext(confirmContext)
	if err != nil {
		return nil, errs.New(errs.RetBadRequest,
			"illegel confirm_context content: unregular confirm_context value format")
	}
	topic, err := parsePartitionKeyToTopic(partitionKey)
	if err != nil {
		return nil, errs.New(errs.RetBadRequest, err.Error())
	}
	if !c.rmtDataCache.IsPartitionInUse(partitionKey, bookedTime) {
		return nil, errs.New(errs.RetErrConfirmTimeout, "The confirm_context's value invalid!")
	}
	partition := c.rmtDataCache.GetPartition(partitionKey)
	if partition == nil {
		return nil, errs.New(errs.RetErrConfirmTimeout, "Not found the partition by confirm_context!")
	}

	defer c.rmtDataCache.ReleasePartition(true, c.subInfo.IsFiltered(topic), confirmContext, consumed)
	rsp, err := c.sendConfirmReq2Broker(partition, consumed)
	if err != nil {
		log.Infof("[CONSUMER]Confirm error %s", err.Error())
		return nil, err
	}

	pi := &PeerInfo{
		BrokerHost:   partition.GetBroker().GetHost(),
		PartitionID:  uint32(partition.GetPartitionID()),
		PartitionKey: partition.GetPartitionKey(),
		CurrOffset:   rsp.GetCurrOffset(),
		MaxOffset:    rsp.GetMaxOffset(),
	}
	cr := &ConfirmResult{
		ConfirmContext: confirmContext,
		TopicName:      partition.GetTopic(),
		PeerInfo:       pi,
	}
	if !rsp.GetSuccess() {
		return cr, errs.New(rsp.GetErrCode(), rsp.GetErrMsg())
	}
	c.rmtDataCache.BookPartitionInfo(partitionKey, rsp.GetCurrOffset(), rsp.GetMaxOffset())
	return cr, err
}

func (c *consumer) sendConfirmReq2Broker(partition *metadata.Partition,
	consumed bool) (*protocol.CommitOffsetResponseB2C, error) {
	m := &metadata.Metadata{}
	node := &metadata.Node{}
	node.SetHost(util.GetLocalHost())
	node.SetAddress(partition.GetBroker().GetAddress())
	m.SetNode(node)
	sub := &metadata.SubscribeInfo{}
	sub.SetGroup(c.config.Consumer.Group)
	partition.SetLastConsumed(consumed)
	sub.SetPartition(partition)
	m.SetSubscribeInfo(sub)

	ctx, cancel := context.WithTimeout(context.Background(), c.config.Net.ReadTimeout-500)
	defer cancel()

	rsp, err := c.client.CommitOffsetRequestC2B(ctx, m, c.subInfo)
	return rsp, err
}

func parsePartitionKeyToTopic(partitionKey string) (string, error) {
	pos1 := strings.Index(partitionKey, ":")
	if pos1 == -1 {
		return "", fmt.Errorf("illegel confirm_context content: unregular index key value format")
	}
	topic := partitionKey[pos1+1:]
	pos2 := strings.LastIndex(topic, ":")
	if pos2 == -1 {
		return "", fmt.Errorf("illegel confirm_context content: unregular index's topic key value format")
	}
	topic = topic[:pos2]
	return topic, nil
}

// GetCurrConsumedInfo implementation of TubeMQ consumer.
func (c *consumer) GetCurrConsumedInfo() map[string]*remote.ConsumerOffset {
	partitionOffset := c.rmtDataCache.GetCurPartitionOffset()
	consumedInfo := make(map[string]*remote.ConsumerOffset, len(partitionOffset))
	for partitionKey, offset := range partitionOffset {
		co := &remote.ConsumerOffset{
			PartitionKey: partitionKey,
			CurrOffset:   offset.CurrOffset,
			MaxOffset:    offset.MaxOffset,
			UpdateTime:   offset.UpdateTime,
		}
		consumedInfo[partitionKey] = co
	}
	return consumedInfo
}

// GetClientID implementation of TubeMQ consumer.
func (c *consumer) GetClientID() string {
	return c.clientID
}

// Close implementation of TubeMQ consumer.
func (c *consumer) Close() {
	c.closeOnce.Do(func() {
		log.Infof("[CONSUMER]Begin to close consumer, client=%s", c.clientID)
		c.cancel()
		c.heartbeatManager.close()
		c.close2Master()
		c.closeAllBrokers()
		c.client.Close()
		<-c.routineClosed
		log.Infof("[CONSUMER]Consumer has been closed successfully, client=%s", c.clientID)
	})
}

func (c *consumer) processRebalanceEvent(ctx context.Context) {
	log.Info("[CONSUMER]Rebalance event Handler starts!")
	for {
		select {
		case <-ctx.Done():
			log.Info("[CONSUMER] Rebalance event Handler stopped!")
			return
		case event, ok := <-c.rmtDataCache.EventCh:
			if ok {
				log.Infof("%+v", event)
				c.rmtDataCache.ClearEvent()
				switch event.GetEventType() {
				case metadata.Disconnect, metadata.OnlyDisconnect:
					c.disconnect2Broker(event)
					c.rmtDataCache.OfferEventResult(event)
				case metadata.Connect, metadata.OnlyConnect:
					c.connect2Broker(ctx, event)
					c.rmtDataCache.OfferEventResult(event)
				}
			}
		}
	}
}

func (c *consumer) disconnect2Broker(event *metadata.ConsumerEvent) {
	log.Tracef("[disconnect2Broker] begin to process disconnect event, client=%s", c.clientID)
	subscribeInfo := event.GetSubscribeInfo()
	if len(subscribeInfo) > 0 {
		removedPartitions := make(map[*metadata.Node][]*metadata.Partition)
		c.rmtDataCache.RemoveAndGetPartition(subscribeInfo,
			c.config.Consumer.RollbackIfConfirmTimeout, removedPartitions)
		if len(removedPartitions) > 0 {
			c.unregister2Broker(removedPartitions)
		}
	}
	event.SetEventStatus(metadata.Done)
	log.Tracef("[disconnect2Broker] disconnect event finished, client=%s", c.clientID)
}

func (c *consumer) unregister2Broker(unRegPartitions map[*metadata.Node][]*metadata.Partition) {
	if len(unRegPartitions) == 0 {
		return
	}
	var wg sync.WaitGroup
	for _, partitions := range unRegPartitions {
		for _, partition := range partitions {
			log.Tracef("unregister2Brokers, partition key=%s", partition.GetPartitionKey())
			partition := partition
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := c.sendUnregisterReq2Broker(partition); err != nil {
					log.Errorf("[CONSUMER] unregister partition %+v failed, error %v", partition, err)
				} else {
					log.Infof("[connect2Broker] unregister partition %+v success", partition)
				}
			}()
		}
	}
	wg.Wait()
}

func (c *consumer) sendUnregisterReq2Broker(partition *metadata.Partition) error {
	ctx, cancel := context.WithTimeout(context.Background(), c.config.Net.ReadTimeout)
	defer cancel()

	m := &metadata.Metadata{}
	node := &metadata.Node{}
	node.SetHost(util.GetLocalHost())
	node.SetAddress(partition.GetBroker().GetAddress())
	m.SetNode(node)
	m.SetReadStatus(1)
	sub := &metadata.SubscribeInfo{}
	sub.SetGroup(c.config.Consumer.Group)
	sub.SetConsumerID(c.clientID)
	sub.SetPartition(partition)
	m.SetSubscribeInfo(sub)
	auth := c.genBrokerAuthenticInfo(true)
	c.subInfo.SetAuthorizedInfo(auth)

	rsp, err := c.client.UnregisterRequestC2B(ctx, m, c.subInfo)
	if err != nil {
		log.Errorf("[CONSUMER] fail to unregister partition %s, error %s", partition, err.Error())
		return err
	}
	if !rsp.GetSuccess() {
		log.Errorf("[CONSUMER] fail to unregister partition %s, err code: %d, error msg %s",
			partition, rsp.GetErrCode(), rsp.GetErrMsg())
		return errs.New(rsp.GetErrCode(), rsp.GetErrMsg())
	}
	return nil
}

func (c *consumer) connect2Broker(ctx context.Context, event *metadata.ConsumerEvent) {
	log.Tracef("[connect2Broker] connect event begin, client=%s", c.clientID)
	if len(event.GetSubscribeInfo()) > 0 {
		unsubPartitions := c.rmtDataCache.FilterPartitions(event.GetSubscribeInfo())
		n := len(unsubPartitions)
		if len(unsubPartitions) > 0 {
			for i, partition := range unsubPartitions {
				select {
				case <-ctx.Done():
					return
				default:
				}

				node := &metadata.Node{}
				node.SetHost(util.GetLocalHost())
				node.SetAddress(partition.GetBroker().GetAddress())

				rsp, err := c.sendRegisterReq2Broker(partition, node)
				if err != nil {
					log.Warnf("[connect2Broker] error %s", err.Error())
					continue
				}
				if !rsp.GetSuccess() {
					log.Warnf("[connect2Broker] err code:%d, err msg: %s", rsp.GetErrCode(), rsp.GetErrMsg())
					return
				}

				log.Infof("[connect2Broker] %v/%v register partition %+v success", i, n, partition)
				c.rmtDataCache.AddNewPartition(partition)
				c.heartbeatManager.registerBroker(partition.GetBroker())
			}
		}
	}
	c.subInfo.SetNotFirstRegistered()
	event.SetEventStatus(metadata.Done)
	log.Tracef("[connect2Broker] connect event finished, client ID=%s", c.clientID)
}

func (c *consumer) sendRegisterReq2Broker(partition *metadata.Partition,
	node *metadata.Node) (*protocol.RegisterResponseB2C, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.config.Net.ReadTimeout)
	defer cancel()

	m := &metadata.Metadata{}
	m.SetNode(node)
	sub := &metadata.SubscribeInfo{}
	sub.SetGroup(c.config.Consumer.Group)
	sub.SetConsumerID(c.clientID)
	sub.SetPartition(partition)
	m.SetSubscribeInfo(sub)
	isFirstRegister := c.rmtDataCache.IsFirstRegister(partition.GetPartitionKey())
	m.SetReadStatus(c.getConsumeReadStatus(isFirstRegister))
	auth := c.genBrokerAuthenticInfo(true)
	c.subInfo.SetAuthorizedInfo(auth)

	rsp, err := c.client.RegisterRequestC2B(ctx, m, c.subInfo, c.rmtDataCache)
	return rsp, err
}

func newClient(group string) string {
	return group + "_" +
		util.GetLocalHost() + "-" +
		strconv.Itoa(os.Getpid()) + "-" +
		strconv.Itoa(int(time.Now().Unix()*1000)) + "-" +
		strconv.Itoa(int(atomic.AddUint64(&clientID, 1))) + "-" +
		"go-" +
		tubeMQClientVersion
}

func (c *consumer) genBrokerAuthenticInfo(force bool) *protocol.AuthorizedInfo {
	needAdd := false
	auth := &protocol.AuthorizedInfo{
		VisitAuthorizedToken: proto.Int64(atomic.LoadInt64(&c.visitToken)),
	}
	if c.config.Net.Auth.Enable {
		if force {
			needAdd = true
			atomic.StoreInt32(&c.nextAuth2Broker, 0)
		} else if atomic.LoadInt32(&c.nextAuth2Broker) == 1 {
			if atomic.CompareAndSwapInt32(&c.nextAuth2Broker, 1, 0) {
				needAdd = true
			}
		}
		if needAdd {
			authToken := util.GenBrokerAuthenticateToken(c.config.Net.Auth.UserName, c.config.Net.Auth.Password)
			auth.AuthAuthorizedToken = proto.String(authToken)
		}
	}
	return auth
}

func (c *consumer) needGenMasterCertificateInfo(force bool) bool {
	needAdd := false
	if c.config.Net.Auth.Enable {
		if force {
			needAdd = true
			atomic.StoreInt32(&c.nextAuth2Master, 0)
		} else if atomic.LoadInt32(&c.nextAuth2Master) == 1 {
			if atomic.CompareAndSwapInt32(&c.nextAuth2Master, 1, 0) {
				needAdd = true
			}
		}
		if needAdd {
		}
	}
	return needAdd
}

func (c *consumer) getConsumeReadStatus(isFirstReg bool) int32 {
	readStatus := consumeStatusNormal
	if isFirstReg {
		if c.config.Consumer.ConsumePosition == 0 {
			readStatus = consumeStatusFromMax
			log.Infof("[Consumer From Max Offset], client=%s", c.clientID)
		} else if c.config.Consumer.ConsumePosition > 0 {
			readStatus = consumeStatusFromMaxAlways
			log.Infof("[Consumer From Max Offset Always], client=%s", c.clientID)
		}
	}
	return int32(readStatus)
}

func (c *consumer) checkPartitionErr() error {
	startTime := time.Now().UnixNano() / int64(time.Millisecond)
	for {
		ret := c.rmtDataCache.GetCurConsumeStatus()
		if ret == 0 {
			return nil
		}
		if c.config.Consumer.MaxPartCheckPeriod >= 0 &&
			time.Now().UnixNano()/int64(time.Millisecond)-startTime >= c.config.Consumer.MaxPartCheckPeriod.Milliseconds() {
			switch ret {
			case errs.RetErrNoPartAssigned:
				return errs.ErrNoPartAssigned
			case errs.RetErrAllPartInUse:
				return errs.ErrAllPartInUse
			case errs.RetErrAllPartWaiting:
				return errs.ErrAllPartWaiting
			}
		}
		time.Sleep(c.config.Consumer.PartCheckSlice)
	}
}

func (c *consumer) processGetMessageRspB2C(pi *PeerInfo, filtered bool, partition *metadata.Partition,
	confirmContext string, rsp *protocol.GetMessageResponseB2C) ([]*Message, error) {
	limitDlt := int64(300)
	escLimit := rsp.GetEscFlowCtrl()
	now := time.Now().UnixNano() / int64(time.Millisecond)
	switch rsp.GetErrCode() {
	case errs.RetSuccess:
		dataDleVal := util.InvalidValue
		if rsp.GetCurrDataDlt() >= 0 {
			dataDleVal = rsp.GetCurrDataDlt()
		}
		currOffset := util.InvalidValue
		if rsp.GetCurrOffset() >= 0 {
			currOffset = rsp.GetCurrOffset()
		}
		maxOffset := util.InvalidValue
		if rsp.GetMaxOffset() >= 0 {
			maxOffset = rsp.GetMaxOffset()
		}
		msgSize, msgs := c.convertMessages(filtered, partition.GetTopic(), rsp)
		c.rmtDataCache.BookPartitionInfo(partition.GetPartitionKey(), currOffset, maxOffset)
		cd := metadata.NewConsumeData(now, 200, escLimit, int32(msgSize), 0,
			dataDleVal, rsp.GetRequireSlow())
		c.rmtDataCache.BookConsumeData(partition.GetPartitionKey(), cd)
		pi.CurrOffset = currOffset
		pi.MaxOffset = maxOffset
		log.Tracef("[CONSUMER] getMessage count=%ld, from %s, client=%s", len(msgs),
			partition.GetPartitionKey(), c.clientID)
		return msgs, nil
	case errs.RetErrHBNoNode, errs.RetCertificateFailure, errs.RetErrDuplicatePartition:
		partitionKey, _, err := util.ParseConfirmContext(confirmContext)
		if err != nil {
			return nil, err
		}
		c.rmtDataCache.RemovePartition([]string{partitionKey})
		return nil, errs.New(rsp.GetErrCode(), rsp.GetErrMsg())
	case errs.RetErrConsumeSpeedLimit:
		defDltTime := int64(rsp.GetMinLimitTime())
		if defDltTime == 0 {
			defDltTime = c.config.Consumer.MsgNotFoundWait.Milliseconds()
		}
		cd := metadata.NewConsumeData(now, rsp.GetErrCode(), false, 0,
			limitDlt, defDltTime, rsp.GetRequireSlow())
		c.rmtDataCache.BookPartitionInfo(partition.GetPartitionKey(), util.InvalidValue, util.InvalidValue)
		c.rmtDataCache.BookConsumeData(partition.GetPartitionKey(), cd)
		c.rmtDataCache.ReleasePartition(true, filtered, confirmContext, false)
		return nil, errs.New(rsp.GetErrCode(), rsp.GetErrMsg())
	case errs.RetErrNotFound:
		limitDlt = c.config.Consumer.MsgNotFoundWait.Milliseconds()
	case errs.RetErrForbidden:
		limitDlt = 2000
	case errs.RetErrMoved:
		limitDlt = 200
	case errs.RetErrServiceUnavailable:
	}
	if rsp.GetErrCode() != errs.RetSuccess {
		cd := metadata.NewConsumeData(now, rsp.GetErrCode(), false, 0,
			limitDlt, util.InvalidValue, rsp.GetRequireSlow())
		c.rmtDataCache.BookPartitionInfo(partition.GetPartitionKey(), util.InvalidValue, util.InvalidValue)
		c.rmtDataCache.BookConsumeData(partition.GetPartitionKey(), cd)
		c.rmtDataCache.ReleasePartition(true, filtered, confirmContext, false)
		return nil, errs.New(rsp.GetErrCode(), rsp.GetErrMsg())
	}
	return nil, errs.New(rsp.GetErrCode(), rsp.GetErrMsg())
}

func (c *consumer) convertMessages(filtered bool, topic string, rsp *protocol.GetMessageResponseB2C) (int, []*Message) {
	msgSize := 0
	if len(rsp.GetMessages()) == 0 {
		return msgSize, nil
	}

	msgs := make([]*Message, 0, len(rsp.GetMessages()))
	for _, m := range rsp.GetMessages() {
		checkSum := uint64(crc32.Update(0, crc32.IEEETable, m.GetPayLoadData())) & 0x7FFFFFFF
		if int32(checkSum) != m.GetCheckSum() {
			continue
		}
		readPos := 0
		payLoadData := m.GetPayLoadData()
		dataLen := len(payLoadData)
		var properties map[string]string
		if m.GetFlag()&msgFlagIncProperties == 1 {
			if len(payLoadData) < 4 {
				continue
			}
			attrLen := int(binary.BigEndian.Uint32(payLoadData[:4]))
			readPos += 4
			dataLen -= 4
			if attrLen > dataLen {
				continue
			}

			attribute := payLoadData[readPos : readPos+attrLen]
			readPos += attrLen
			dataLen -= attrLen
			properties = util.SplitToMap(string(attribute), ",", "=")
			if filtered {
				topicFilters := c.subInfo.GetTopicFilters()
				if msgKey, ok := properties["$msgType$"]; ok {
					if filters, ok := topicFilters[topic]; ok {
						found := false
						for _, filter := range filters {
							if filter == msgKey {
								found = true
								break
							}
						}
						if !found {
							continue
						}
					}
				}
			}
		}
		msg := &Message{
			Topic:      topic,
			Flag:       m.GetFlag(),
			ID:         m.GetMessageId(),
			Properties: properties,
			DataLen:    int32(dataLen),
			Data:       payLoadData[readPos:],
		}
		msgs = append(msgs, msg)
		msgSize += dataLen
	}
	return msgSize, msgs
}

func (c *consumer) close2Master() {
	log.Infof("[CONSUMER] close2Master begin, client=%s", c.clientID)
	ctx, cancel := context.WithTimeout(context.Background(), c.config.Net.ReadTimeout)
	defer cancel()

	m := &metadata.Metadata{}
	node := &metadata.Node{}
	node.SetHost(util.GetLocalHost())
	node.SetAddress(c.master.Address)
	m.SetNode(node)
	sub := &metadata.SubscribeInfo{}
	sub.SetGroup(c.config.Consumer.Group)
	m.SetSubscribeInfo(sub)
	mci := &protocol.MasterCertificateInfo{}
	auth := &protocol.AuthenticateInfo{}
	if c.needGenMasterCertificateInfo(true) {
		util.GenMasterAuthenticateToken(auth, c.config.Net.Auth.UserName, c.config.Net.Auth.Password)
	}
	c.subInfo.SetMasterCertificateInfo(mci)
	rsp, err := c.client.CloseRequestC2M(ctx, m, c.subInfo)
	if err != nil {
		log.Errorf("[CONSUMER] fail to close master, error: %s", err.Error())
		return
	}
	if !rsp.GetSuccess() {
		log.Errorf("[CONSUMER] fail to close master, error code: %d, error msg: %s",
			rsp.GetErrCode(), rsp.GetErrMsg())
		return
	}
	log.Infof("[CONSUMER] close2Master finished, client=%s", c.clientID)
}

func (c *consumer) closeAllBrokers() {
	log.Infof("[CONSUMER] closeAllBrokers begin, client=%s", c.clientID)
	partitions := c.rmtDataCache.GetAllClosedBrokerParts()
	if len(partitions) > 0 {
		c.unregister2Broker(partitions)
	}
	log.Infof("[CONSUMER] closeAllBrokers end, client=%s", c.clientID)
}
