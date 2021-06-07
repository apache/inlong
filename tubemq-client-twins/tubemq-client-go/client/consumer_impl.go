/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package client

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/config"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/errs"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/metadata"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/multiplexing"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/protocol"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/remote"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/rpc"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/selector"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/sub"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/transport"
	"github.com/apache/incubator-inlong/tubemq-client-twins/tubemq-client-go/util"
)

const (
	consumeStatusNormal        = 0
	consumeStatusFromMax       = 1
	consumeStatusFromMaxAlways = 2
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
}

// NewConsumer returns a consumer which is constructed by a given config.
func NewConsumer(config *config.Config) (Consumer, error) {
	selector, err := selector.Get("ip")
	if err != nil {
		return nil, err
	}

	clientID := newClientID(config.Consumer.Group)
	pool := multiplexing.NewPool()
	opts := &transport.Options{}
	if config.Net.TLS.Enable {
		opts.CACertFile = config.Net.TLS.CACertFile
		opts.TLSCertFile = config.Net.TLS.TLSCertFile
		opts.TLSKeyFile = config.Net.TLS.TLSKeyFile
		opts.TLSServerName = config.Net.TLS.TLSServerName
	}
	client := rpc.New(pool, opts)
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
	}
	c.subInfo.SetClientID(clientID)
	hbm := newHBManager(c)
	c.heartbeatManager = hbm
	return c, nil
}

// Start implementation of tubeMQ consumer.
func (c *consumer) Start() error {
	err := c.register2Master(false)
	if err != nil {
		return err
	}
	c.heartbeatManager.registerMaster(c.master.Address)
	go c.processRebalanceEvent()
	return nil
}

func (c *consumer) register2Master(needChange bool) error {
	if needChange {
		node, err := c.selector.Select(c.config.Consumer.Masters)
		if err != nil {
			return err
		}
		c.master = node
	}
	for c.master.HasNext {
		ctx, cancel := context.WithTimeout(context.Background(), c.config.Net.ReadTimeout)

		m := &metadata.Metadata{}
		node := &metadata.Node{}
		node.SetHost(util.GetLocalHost())
		node.SetAddress(c.master.Address)
		m.SetNode(node)
		sub := &metadata.SubscribeInfo{}
		sub.SetGroup(c.config.Consumer.Group)
		m.SetSubscribeInfo(sub)

		auth := &protocol.AuthenticateInfo{}
		c.genMasterAuthenticateToken(auth, true)
		mci := &protocol.MasterCertificateInfo{
			AuthInfo: auth,
		}
		c.subInfo.SetMasterCertificateInfo(mci)

		rsp, err := c.client.RegisterRequestC2M(ctx, m, c.subInfo, c.rmtDataCache)
		if err != nil {
			cancel()
			return err
		}
		if rsp.GetSuccess() {
			c.masterHBRetry = 0
			c.processRegisterResponseM2C(rsp)
			cancel()
			return nil
		} else if rsp.GetErrCode() == errs.RetConsumeGroupForbidden || rsp.GetErrCode() == errs.RetConsumeContentForbidden {
			cancel()
			return nil
		} else {
			c.master, err = c.selector.Select(c.config.Consumer.Masters)
			cancel()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *consumer) processRegisterResponseM2C(rsp *protocol.RegisterResponseM2C) {
	if rsp.GetNotAllocated() {
		c.subInfo.SetIsNotAllocated(rsp.GetNotAllocated())
	}
	if rsp.GetDefFlowCheckId() != 0 || rsp.GetDefFlowCheckId() != 0 {
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
	c.lastMasterHb = time.Now().UnixNano() / int64(time.Millisecond)
}

func (c *consumer) processAuthorizedToken(info *protocol.MasterAuthorizedInfo) {
	atomic.StoreInt64(&c.visitToken, info.GetVisitAuthorizedToken())
	c.authorizedInfo = info.GetAuthAuthorizedToken()
}

// GetMessage implementation of TubeMQ consumer.
func (c *consumer) GetMessage() (*ConsumerResult, error) {
	err := c.checkPartitionErr()
	if err != nil {
		return nil, err
	}
	partition, err := c.rmtDataCache.SelectPartition()
	if err != nil {
		return nil, err
	}
	confirmContext := partition.GetPartitionKey() + "@" + strconv.Itoa(int(time.Now().UnixNano()/int64(time.Millisecond)))
	isFiltered := c.subInfo.IsFiltered(partition.GetTopic())
	pi := &PeerInfo{
		brokerHost:   partition.GetBroker().GetHost(),
		partitionID:  uint32(partition.GetPartitionID()),
		partitionKey: partition.GetPartitionKey(),
		currOffset:   util.InvalidValue,
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
		return nil, err
	}
	cs := &ConsumerResult{
		topicName:      partition.GetTopic(),
		confirmContext: confirmContext,
		peerInfo:       pi,
	}
	if !rsp.GetSuccess() {
		err := c.rmtDataCache.ReleasePartition(true, isFiltered, confirmContext, false)
		return cs, err
	}
	msgs, err := c.processGetMessageRspB2C(pi, isFiltered, partition, confirmContext, rsp)
	if err != nil {
		return cs, err
	}
	cs.messages = msgs
	return cs, err
}

// Confirm implementation of TubeMQ consumer.
func (c *consumer) Confirm(confirmContext string, consumed bool) (*ConsumerResult, error) {
	panic("implement me")
}

// GetCurrConsumedInfo implementation of TubeMQ consumer.
func (c *consumer) GetCurrConsumedInfo() (map[string]*ConsumerOffset, error) {
	panic("implement me")
}

func (c *consumer) processRebalanceEvent() {
	for {
		event := c.rmtDataCache.TakeEvent()
		if event.GetEventStatus() == int32(util.InvalidValue) && event.GetRebalanceID() == util.InvalidValue {
			break
		}
		c.rmtDataCache.ClearEvent()
		switch event.GetEventType() {
		case 2, 20:
			c.disconnect2Broker(event)
			c.rmtDataCache.OfferEventResult(event)
		case 1, 10:
			c.connect2Broker(event)
			c.rmtDataCache.OfferEventResult(event)
		}
	}
}

func (c *consumer) disconnect2Broker(event *metadata.ConsumerEvent) {
	subscribeInfo := event.GetSubscribeInfo()
	if len(subscribeInfo) > 0 {
		removedPartitions := make(map[*metadata.Node][]*metadata.Partition)
		c.rmtDataCache.RemoveAndGetPartition(subscribeInfo, c.config.Consumer.RollbackIfConfirmTimeout, removedPartitions)
		if len(removedPartitions) > 0 {
			c.unregister2Broker(removedPartitions)
		}
	}
	event.SetEventStatus(2)
}

func (c *consumer) unregister2Broker(unRegPartitions map[*metadata.Node][]*metadata.Partition) {
	if len(unRegPartitions) == 0 {
		return
	}

	for _, partitions := range unRegPartitions {
		for _, partition := range partitions {
			ctx, cancel := context.WithTimeout(context.Background(), c.config.Net.ReadTimeout)

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

			c.client.UnregisterRequestC2B(ctx, m, c.subInfo)
			cancel()
		}
	}
}

func (c *consumer) connect2Broker(event *metadata.ConsumerEvent) {
	if len(event.GetSubscribeInfo()) > 0 {
		unsubPartitions := c.rmtDataCache.FilterPartitions(event.GetSubscribeInfo())
		if len(unsubPartitions) > 0 {
			for _, partition := range unsubPartitions {
				m := &metadata.Metadata{}
				node := &metadata.Node{}
				node.SetHost(util.GetLocalHost())
				node.SetAddress(partition.GetBroker().GetAddress())
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

				ctx, cancel := context.WithTimeout(context.Background(), c.config.Net.ReadTimeout)
				rsp, err := c.client.RegisterRequestC2B(ctx, m, c.subInfo, c.rmtDataCache)
				if err != nil {
					//todo add log
				}
				if rsp.GetSuccess() {
					c.rmtDataCache.AddNewPartition(partition)
					c.heartbeatManager.registerBroker(node)
				}
				cancel()
			}
		}
	}
	c.subInfo.FirstRegistered()
	event.SetEventStatus(2)
}

func newClientIndex() uint64 {
	return atomic.AddUint64(&clientIndex, 1)
}

func newClientID(group string) string {
	return group + "_" +
		util.GetLocalHost() + "_" +
		strconv.Itoa(os.Getpid()) + "_" +
		strconv.Itoa(int(time.Now().Unix()*1000)) + "_" +
		strconv.Itoa(int(newClientIndex())) + "_" +
		tubeMQClientVersion
}

func (c *consumer) genBrokerAuthenticInfo(force bool) *protocol.AuthorizedInfo {
	needAdd := false
	auth := &protocol.AuthorizedInfo{}
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

func (c *consumer) genMasterAuthenticateToken(auth *protocol.AuthenticateInfo, force bool) {
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
}

func (c *consumer) getConsumeReadStatus(isFirstReg bool) int32 {
	readStatus := consumeStatusNormal
	if isFirstReg {
		if c.config.Consumer.ConsumePosition == 0 {
			readStatus = consumeStatusFromMax
		} else if c.config.Consumer.ConsumePosition > 0 {
			readStatus = consumeStatusFromMaxAlways
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

func (c *consumer) processGetMessageRspB2C(pi *PeerInfo, filtered bool, partition *metadata.Partition, confirmContext string, rsp *protocol.GetMessageResponseB2C) ([]*Message, error) {
	limitDlt := int64(300)
	escLimit := rsp.GetEscFlowCtrl()
	now := time.Now().UnixNano() / int64(time.Millisecond)
	switch rsp.GetErrCode() {
	case 200:
		dataDleVal := util.InvalidValue
		if rsp.GetCurrDataDlt() >= 0 {
			dataDleVal = rsp.GetCurrDataDlt()
		}
		currOffset := util.InvalidValue
		if rsp.GetCurrOffset() >= 0 {
			currOffset = rsp.GetCurrOffset()
		}
		msgSize, msgs := c.convertMessages(filtered, partition.GetTopic(), rsp)
		c.rmtDataCache.BookPartitionInfo(partition.GetPartitionKey(), currOffset)
		cd := metadata.NewConsumeData(time.Duration(now)*time.Millisecond, 200, escLimit, int32(msgSize), 0, dataDleVal, rsp.GetRequireSlow())
		c.rmtDataCache.BookConsumeData(partition.GetPartitionKey(), cd)
		pi.currOffset = currOffset
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
		cd := metadata.NewConsumeData(time.Duration(now), rsp.GetErrCode(), false, 0, limitDlt, defDltTime, rsp.GetRequireSlow())
		c.rmtDataCache.BookPartitionInfo(partition.GetPartitionKey(), util.InvalidValue)
		c.rmtDataCache.BookConsumeData(partition.GetPartitionKey(), cd)
		return nil, errs.New(rsp.GetErrCode(), rsp.GetErrMsg())
	case errs.RetErrNotFound:
		limitDlt = c.config.Consumer.MsgNotFoundWait.Milliseconds()
	case errs.RetErrForbidden:
		limitDlt = 2000
	case errs.RetErrMoved:
		limitDlt = 200
	case errs.RetErrServiceUnavailable:
	}
	if rsp.GetErrCode() != 200 {
		cd := metadata.NewConsumeData(time.Duration(now), rsp.GetErrCode(), false, 0, limitDlt, util.InvalidValue, rsp.GetRequireSlow())
		c.rmtDataCache.BookPartitionInfo(partition.GetPartitionKey(), util.InvalidValue)
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
		checkSum := uint64(crc32.Update(0, crc32.IEEETable, m.GetPayLoadData())) & 0x7FFFFFFFF
		if int32(checkSum) != m.GetCheckSum() {
			continue
		}
		readPos := 0
		dataLen := len(m.GetPayLoadData())
		var properties map[string]string
		if m.GetFlag()&0x01 == 1 {
			if len(m.GetPayLoadData()) < 4 {
				continue
			}
			attrLen := int(binary.BigEndian.Uint64(m.GetPayLoadData()))
			readPos += 4
			dataLen -= 4

			attribute := m.GetPayLoadData()[readPos:readPos+attrLen]
			readPos -= attrLen
			dataLen -= attrLen
			properties := util.SplitToMap(string(attribute), ",", "=")
			if filtered {
				topicFilters := c.subInfo.GetTopicFilters()
				if msgKey, ok := properties["$msgType$"]; ok {
					if filters, ok := topicFilters[topic]; ok {
						for _, filter := range filters {
							if filter == msgKey {
								continue
							}
						}
					}
				}
			}
		}
		msg := &Message{
			topic:      topic,
			flag:       m.GetFlag(),
			id:         m.GetMessageId(),
			properties: properties,
			dataLen:    int32(dataLen),
			data:       string(m.GetPayLoadData()[:readPos]),
		}
		msgs = append(msgs, msg)
		msgSize += dataLen
	}
	return msgSize, msgs
}
