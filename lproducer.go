package nsq

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// LProducer encapsulates a layer of Producer
// implements failed reconnection and discovery of all nsqd in the cluster
//type LProducer interface {
//	// Ping causes current Producer to connect to it's configured nsqd (if not already
//	// connected) and send a `Nop` command, returning any error that might occur.
//	// If it fails, it will reconnect to the available nsqd
//	Ping() error
//	// SetLogger assigns the logger to use as well as a level and current Producer logger and level
//	SetLogger(l logger, level LogLevel)
//	// String returns the address of current Producer
//	String() string
//	// StopCurrentProducer initiates a graceful stop of the current Producer and clear the nsqd address cache
//	// If released again, new producers will be reconnected
//	StopCurrentProducer()
//	// Stop Real stop LProducer, No more reconnections
//	Stop()
//	// PublishAsync publishes a message body to the specified topic
//	// but does not wait for the response from `nsqd`.
//	PublishAsync(topic string, body []byte, doneChan chan *ProducerTransaction, args ...interface{}) error
//	// MultiPublishAsync publishes a slice of message bodies to the specified topic
//	// but does not wait for the response from `nsqd`.
//	MultiPublishAsync(topic string, body [][]byte, doneChan chan *ProducerTransaction, args ...interface{}) error
//	// Publish synchronously publishes a message body to the specified topic, returning
//	// an error if publish failed and retry failed
//	Publish(topic string, body []byte) error
//	// MultiPublish synchronously publishes a slice of message bodies to the specified topic, returning
//	// an error if publish failed and retry failed
//	MultiPublish(topic string, body [][]byte) error
//	// DeferredPublish synchronously publishes a message body to the specified topic
//	// where the message will queue at the channel level until the timeout expires, returning
//	// an error if publish failed and retry failed
//	DeferredPublish(topic string, delay time.Duration, body []byte) error
//	// DeferredPublishAsync publishes a message body to the specified topic
//	// where the message will queue at the channel level until the timeout expires
//	// but does not wait for the response from `nsqd`.
//	DeferredPublishAsync(topic string, delay time.Duration, body []byte,
//		doneChan chan *ProducerTransaction, args ...interface{}) error
//}

type nsqdNodes struct {
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	//...Omit unnecessary fields
}

type nodesData struct {
	Producers []nsqdNodes `json:"producers"`
}

type LProducer struct {
	lookupAdders       []string
	availableNSQDNodes map[string]struct{}

	cfg                *Config
	logger             logger
	logLvl             LogLevel

	logGuard           sync.RWMutex
	guard              sync.RWMutex

	currentProducer    *Producer
	stopFlag           int32
}

// StopCurrentProducer initiates a graceful stop of the Producer (permanent) and clear the nsqd address cache
func (n *LProducer) StopCurrentProducer() {
	if n.currentProducer != nil {
		n.currentProducer.Stop()
	}

	n.currentProducer = nil
	n.availableNSQDNodes = nil
}

func (n *LProducer) Stop() {
	n.guard.Lock()
	if n.currentProducer != nil {
		n.currentProducer.Stop()
	}
	if !atomic.CompareAndSwapInt32(&n.stopFlag, 0, 1) {
		n.guard.Unlock()
		return
	}

	n.currentProducer = nil
	n.availableNSQDNodes = nil
	n.lookupAdders = nil
	n.guard.Unlock()
}

// SetLogger assigns the logger to use as well as a level and current Producer logger and level
func (n *LProducer) SetLogger(l logger, level LogLevel) {
	n.logGuard.Lock()
	defer n.logGuard.Unlock()
	n.logger = l
	n.logLvl = level
	if n.currentProducer != nil {
		n.currentProducer.SetLogger(l, level)
	}
}

func (n *LProducer) getLogger() (logger, LogLevel) {
	n.logGuard.RLock()
	defer n.logGuard.RUnlock()

	return n.logger, n.logLvl
}

// 给nsq生产者赋予Log
func (n *LProducer) setProducerLog() {
	n.logGuard.RLock()
	defer n.logGuard.RUnlock()
	if n.logger != nil && n.currentProducer != nil {
		n.currentProducer.SetLogger(n.logger, n.logLvl)
	}
}

// Ping causes current Producer to connect to it's configured nsqd (if not already
// connected) and send a `Nop` command, returning any error that might occur.
//
// If it fails, it will reconnect to the available nsqd
func (n *LProducer) Ping() (err error) {
	if err = n.checkProducer(); err != nil {
		return
	}
	if err = n.currentProducer.Ping(); err != nil {
		// 尝试再次获取生产者
		if err = n.getProducerWithRetry(); err != nil {
			return err
		}
		return n.currentProducer.Ping()
	}
	return
}

func (n *LProducer) checkProducer() error {
	if n.currentProducer == nil {
		if err := n.getProducerWithRetry(); err != nil {
			return err
		}
	}
	return nil
}

// Publish synchronously publishes a message body to the specified topic, returning
// an error if publish failed and retry failed
func (n *LProducer) Publish(topic string, body []byte) (err error) {
	if err = n.checkProducer(); err != nil {
		return
	}
	//If the publish fails, the publish will be retried
	if err = n.currentProducer.Publish(topic, body); err != nil {
		// 获取新的生产者,并设置为当前生产者
		if err = n.getProducerWithRetry(); err != nil {
			return err
		}
		return n.currentProducer.Publish(topic, body)
	}
	return nil
}

// String returns the address of current Producer
func (n *LProducer) String() string {
	if err := n.checkProducer(); err != nil {
		return ""
	}
	return n.currentProducer.String()
}

// PublishAsync publishes a message body to the specified topic
// but does not wait for the response from `nsqd`.
func (n *LProducer) PublishAsync(topic string, body []byte, doneChan chan *ProducerTransaction,
	args ...interface{}) (err error) {
	if err = n.checkProducer(); err != nil {
		return
	}
	//If the publish fails, the publish will be retried
	if err = n.currentProducer.PublishAsync(topic, body, doneChan, args...); err != nil {
		// Get the new producer and set it as the current producer
		if err = n.getProducerWithRetry(); err != nil {
			return err
		}
		return n.currentProducer.PublishAsync(topic, body, doneChan, args...)
	}
	return nil
}

// MultiPublishAsync publishes a slice of message bodies to the specified topic
// but does not wait for the response from `nsqd`.
func (n *LProducer) MultiPublishAsync(topic string, body [][]byte, doneChan chan *ProducerTransaction,
	args ...interface{}) (err error) {
	if err = n.checkProducer(); err != nil {
		return
	}
	//If the publish fails, the publish will be retried
	if err = n.currentProducer.MultiPublishAsync(topic, body, doneChan, args...); err != nil {
		// Get the new producer and set it as the current producer
		if err = n.getProducerWithRetry(); err != nil {
			return err
		}
		return n.currentProducer.MultiPublishAsync(topic, body, doneChan, args...)
	}
	return nil
}

// DeferredPublish synchronously publishes a message body to the specified topic
// where the message will queue at the channel level until the timeout expires, returning
// an error if publish failed and retry failed
func (n *LProducer) DeferredPublish(topic string, delay time.Duration, body []byte) (err error) {
	// Check current producer not nil
	if err = n.checkProducer(); err != nil {
		return
	}
	//If the publish fails, the publish will be retried
	if err = n.currentProducer.DeferredPublish(topic, delay, body); err != nil {
		// Get the new producer and set it as the current producer
		if err = n.getProducerWithRetry(); err != nil {
			return err
		}
		// retry publish
		return n.currentProducer.DeferredPublish(topic, delay, body)
	}
	return nil
}

// DeferredPublishAsync publishes a message body to the specified topic
// where the message will queue at the channel level until the timeout expires
// but does not wait for the response from `nsqd`.
//
// When the Producer eventually receives the response from `nsqd`,
// the supplied `doneChan` (if specified)
// will receive a `ProducerTransaction` instance with the supplied variadic arguments
// and the response error if present
func (n *LProducer) DeferredPublishAsync(topic string, delay time.Duration, body []byte,
	doneChan chan *ProducerTransaction, args ...interface{}) (err error) {
	// Check current producer not nil
	if err = n.checkProducer(); err != nil {
		return
	}
	//If the publish fails, the publish will be retried
	if err = n.currentProducer.DeferredPublishAsync(topic, delay, body, doneChan, args...); err != nil {
		// Get the new producer and set it as the current producer
		if err = n.getProducerWithRetry(); err != nil {
			return err
		}
		// retry publish
		return n.currentProducer.DeferredPublishAsync(topic, delay, body, doneChan, args...)
	}
	return nil
}

// MultiPublish synchronously publishes a slice of message bodies to the specified topic, returning
// an error if publish failed and retry failed
func (n *LProducer) MultiPublish(topic string, body [][]byte) (err error) {
	// Check current producer not nil
	if err = n.checkProducer(); err != nil {
		return
	}
	//If the publish fails, the publish will be retried
	if err = n.currentProducer.MultiPublish(topic, body); err != nil {
		// Get the new producer and set it as the current producer
		if err = n.getProducerWithRetry(); err != nil {
			return err
		}
		return n.currentProducer.MultiPublish(topic, body)
	}
	return nil
}

//Get an available producer
//If the Ping fails, it will retry until no nsqd nodes are available
func (n *LProducer) getProducer() error {
	if n.availableNSQDNodes == nil ||
		len(n.availableNSQDNodes) == 0 {
		if err := n.getAllAvailableNSQDFromNSQLookupd(); err != nil {
			return err
		}
	}
	var (
		producer *Producer
		err      error
	)
	//Traverse cache nodes, connect producers
	//Random connection through map
	for node := range n.availableNSQDNodes {
		if producer, err = NewProducer(node, n.cfg); err != nil {
			continue
		}
		if err = producer.Ping(); err != nil {
			continue
		}
		//Reset current producer
		n.currentProducer = producer
		//Set up log
		n.setProducerLog()
		break
	}
	return err
}

//Get a new producer.
// If the producers created through the cache node are invalid, reconnect to lookupdaddrs, get the node and try again
//
// Try to use the cached nsqd nodes first, all of which are invalid.
// Then update all node information from nsqlookupd
func (n *LProducer) getProducerWithRetry() error {
	n.guard.Lock()
	defer n.guard.Unlock()
	// Try to use the cached nsqd nodes first, all of which are invalid.
	err := n.getProducer()
	if err != nil {
		n.StopCurrentProducer()
		// Then update all node information from nsqlookupd
		if err = n.getAllAvailableNSQDFromNSQLookupd(); err != nil {
			return err
		}
		err = n.getProducer()
		if err != nil {
			return err
		}
	}
	return err
}

// Get all available nodes from nsqlookupd
func (n *LProducer) getAllAvailableNSQDFromNSQLookupd() error {
	if atomic.LoadInt32(&n.stopFlag) == 1 {
		return ErrStopped
	}
	var (
		results        = make(map[string]struct{})
		tempResult nodesData
	)
	for _, lookupdAddr := range n.lookupAdders {
		// Search all available nodes
		err := apiRequestNegotiateV1("GET", "http://"+lookupdAddr+"/nodes", nil, &tempResult)
		if err != nil {
			n.log(LogLevelError, "(%s) get nsqd nodes - %s", lookupdAddr, err)
		}
		// Cache all resolved addresses into mapCache all resolved addresses into map
		for _, node := range tempResult.Producers {
			results[fmt.Sprintf("%s:%d", node.BroadcastAddress, node.TCPPort)] = struct{}{}
		}
	}
	//Err if no nodes are available
	if len(results) == 0 {
		return ErrLookupNSQD
	}
	n.availableNSQDNodes = results
	return nil
}

// NewLProducer Obtain NSQ node through nsqlookupdaddr, return LProducer,
// realize producer reconnection and discovery of all nodes
func NewLProducer(nsqlookupdAddr string, config *Config) (*LProducer, error) {
	config.assertInitialized()
	err := config.Validate()
	if err != nil {
		return nil, err
	}
	instance := &LProducer{
		lookupAdders: []string{nsqlookupdAddr},
		cfg:          config,
		logger:       log.New(os.Stderr, "", log.Flags()),
		logLvl:       LogLevelInfo,
	}
	//// Get all nodes in the cluster according to nsqlookupdaddrs
	//err = instance.getAllAvailableNSQDFromNSQLookupd()
	//if err != nil {
	//	return nil, err
	//}
	//// Get nodes randomly and be producers
	//err = instance.getProducerWithRetry()

	return instance, err
}

// NewLProducerByLookupdAddrs Obtain NSQ node through nsqlookupdaddrs, return LProducer,
// realize producer reconnection and discovery of all nodes
func NewLProducerByLookupdAddrs(nsqlookupdAddrs []string, config *Config) (*LProducer, error) {
	config.assertInitialized()
	err := config.Validate()
	if err != nil {
		return nil, err
	}
	instance := &LProducer{
		lookupAdders: nsqlookupdAddrs,
		cfg:          config,
		logger:       log.New(os.Stderr, "", log.Flags()),
		logLvl:       LogLevelInfo,
	}
	//// Get all nodes in the cluster according to nsqlookupdaddrs
	//err = instance.getAllAvailableNSQDFromNSQLookupd()
	//if err != nil {
	//	return nil, err
	//}
	//// Get nodes randomly and be producers
	//err = instance.getProducerWithRetry()

	return instance, err
}

func (n *LProducer) log(lvl LogLevel, line string, args ...interface{}) {
	logger, logLvl := n.getLogger()
	if logger == nil {
		return
	}
	if logLvl > lvl {
		return
	}
	logger.Output(2, fmt.Sprintf("%-4s %s", lvl, fmt.Sprintf(line, args...)))
}
