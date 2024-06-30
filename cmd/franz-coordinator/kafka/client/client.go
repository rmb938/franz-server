package client

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/go-logr/logr"
	"github.com/rmb938/franz-server/cmd/franz-coordinator/kafka/protocol"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type client struct {
	log  logr.Logger
	conn net.Conn
}

func NewClient(log logr.Logger, conn net.Conn) *client {
	log = log.WithValues("from-address", conn.RemoteAddr().String())

	return &client{
		log:  log,
		conn: conn,
	}
}

func (c *client) Run() {
	c.log.Info("Accepted Connection")
	defer c.conn.Close()

	for {
		packetSize := make([]byte, 4)
		_, err := io.ReadFull(c.conn, packetSize)
		if err != nil {
			c.log.Error(err, "error reading packet size")
			return
		}

		packetBody := make([]byte, binary.BigEndian.Uint32(packetSize))
		_, err = io.ReadFull(c.conn, packetBody)
		if err != nil {
			c.log.Error(err, "error reading packet body")
			return
		}

		packetReader := kbin.Reader{Src: packetBody}

		// header
		packetAPIKey := packetReader.Int16()
		packetAPIVersion := packetReader.Int16()
		packetCorrelationId := packetReader.Int32()
		packetClientId := packetReader.NullableString()

		log := c.log.WithValues("correlationId", packetCorrelationId, "clientId", packetClientId)

		// packet
		kreq := kmsg.RequestForKey(packetAPIKey)
		kreq.SetVersion(packetAPIVersion)
		if kreq.IsFlexible() {
			kmsg.SkipTags(&packetReader)
		}

		err = kreq.ReadFrom(packetReader.Src)
		if err != nil {
			log.Error(err, "error reading kafka message")
			return
		}

		log.Info("packet request", "kreq", fmt.Sprintf("%#v", kreq))

		kresp, err := protocol.HandleProtocolPacket(kreq)
		if err != nil {
			log.Error(err, "error handling kafka protocol packet")
			return
		}

		log.Info("packet response", "kresp", fmt.Sprintf("%#v", kresp))

		// size - 4 bytes
		// correlation - 4 bytes
		// empty tag section if flexible - 1 byte
		buf := make([]byte, 8)

		// if flexible add one more byte for empty tags
		// except key 18 (api versions), it doesn't have this in the header
		if kresp.IsFlexible() && kresp.Key() != 18 {
			buf = append(buf, make([]byte, 1)...)
		}

		buf = kresp.AppendTo(buf)
		responsePacketLength := len(buf) - 4

		// set response packet length
		binary.BigEndian.PutUint32(buf[0:], uint32(responsePacketLength))

		// set response packet corralationId
		binary.BigEndian.PutUint32(buf[4:], uint32(packetCorrelationId))

		log.Info("response packet bytes", "bytes", fmt.Sprintf("%08b ", buf))

		c.conn.Write(buf)

	}
}
