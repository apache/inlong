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
	done             chan struct{}
}

// NewConsumer returns a consumer which is constructed by a given config.
func NewConsumer(config *config.Config) (Consumer, error) {
	selector, err := selector.Get("ip")
	if err != nil {
		return nil, err
	}

	clientID := newClient(config.Consumer.Group)
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
		done:            make(chan struct{}),
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
		rsp, err := c.sendRegRequest2Master()
		if err != nil {
			return err
		}
		if rsp.GetSuccess() {
			c.masterHBRetry = 0
			c.processRegisterResponseM2C(rsp)
			return nil
		} else if rsp.GetErrCode() == errs.RetConsumeGroupForbidden || rsp.GetErrCode() == errs.RetConsumeContentForbidden {
			return nil
		} else {
			c.master, err = c.selector.Select(c.config.Consumer.Masters)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *consumer) sendRegRequest2Master() (*protocol.RegisterResponseM2C, error) {
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

	auth := &protocol.AuthenticateInfo{}
	c.genMasterAuthenticateToken(auth, true)
	mci := &protocol.MasterCertificateInfo{
		AuthInfo: auth,
	}
	c.subInfo.SetMasterCertificateInfo(mci)

	rsp, err := c.client.RegisterRequestC2M(ctx, m, c.subInfo, c.rmtDataCache)
	return rsp, err
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
	panic("implement me")
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
		select {
		case event, ok := <-c.rmtDataCache.EventCh:
			if ok {
				if event.GetEventStatus() == int32(util.InvalidValue) && event.GetRebalanceID() == util.InvalidValue {
					break
				}
				c.rmtDataCache.ClearEvent()
				switch event.GetEventType() {
				case metadata.Disconnect, metadata.OnlyDisconnect:
					c.disconnect2Broker(event)
					c.rmtDataCache.OfferEventResult(event)
				case metadata.Connect, metadata.OnlyConnect:
					c.connect2Broker(event)
					c.rmtDataCache.OfferEventResult(event)
				}
			}
		case <-c.done:
			break
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
			c.sendUnregisterReq2Broker(partition)
		}
	}
}

func (c *consumer) sendUnregisterReq2Broker(partition *metadata.Partition) {
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

	c.client.UnregisterRequestC2B(ctx, m, c.subInfo)
}

func (c *consumer) connect2Broker(event *metadata.ConsumerEvent) {
	if len(event.GetSubscribeInfo()) > 0 {
		unsubPartitions := c.rmtDataCache.FilterPartitions(event.GetSubscribeInfo())
		if len(unsubPartitions) > 0 {
			for _, partition := range unsubPartitions {

				node := &metadata.Node{}
				node.SetHost(util.GetLocalHost())
				node.SetAddress(partition.GetBroker().GetAddress())

				rsp, err := c.sendRegisterReq2Broker(partition, node)
				if err != nil {
					//todo add log
				}
				if rsp.GetSuccess() {
					c.rmtDataCache.AddNewPartition(partition)
					c.heartbeatManager.registerBroker(node)
				}
			}
		}
	}
	c.subInfo.FirstRegistered()
	event.SetEventStatus(metadata.Disconnect)
}

func (c *consumer) sendRegisterReq2Broker(partition *metadata.Partition, node *metadata.Node) (*protocol.RegisterResponseB2C, error) {
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
		util.GetLocalHost() + "_" +
		strconv.Itoa(os.Getpid()) + "_" +
		strconv.Itoa(int(time.Now().Unix()*1000)) + "_" +
		strconv.Itoa(int(atomic.AddUint64(&clientID, 1))) + "_" +
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
