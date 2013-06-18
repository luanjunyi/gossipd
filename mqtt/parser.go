package mqtt

import ("bytes"
	"errors"
	"log")

func getUint8(b []byte, p *int)uint8{
    *p += 1
    return uint8(b[*p-1])
}

func getUint16(b []byte, p *int)uint16{
    *p += 2
    return uint16(b[*p-2] << 8) + uint16(b[*p-1])
}

func getString(b []byte, p *int)string{
    length := int(getUint16(b, p))
    *p += length
    return string(b[*p-length:*p])
}

func getHeader(b []byte, p *int)*FixedHeader{
    byte1 := b[*p]
    *p += 1
    header := new(FixedHeader)
    header.MessageType = uint8(byte1 & 0xF0 >> 4)
    header.DupFlag = byte1 & 0x08 > 0
    header.QosLevel = uint8(byte1 & 0x06 >> 1)
    header.Retain = byte1 & 0x01 > 0
    header.Length = decodeLength(b, p)
    return header
}

func getConnectFlags(b []byte, p *int)*ConnectFlags{
    bit := b[*p]
    *p += 1
    flags := new(ConnectFlags)
    flags.UsernameFlag = bit & 0x80 > 0
    flags.PasswordFlag = bit & 0x40 > 0
    flags.WillRetain = bit & 0x20 > 0
    flags.WillQos = uint8(bit & 0x18 >> 3)
    flags.WillFlag = bit & 0x04 > 0
    flags.CleanSession = bit & 0x02 > 0
    return flags
}

func DecodeAfterFixedHeader(fixed_header *FixedHeader, buf []byte)(*Mqtt, error) {
    mqtt := new(Mqtt)
	idx := 0
    mqtt.FixedHeader = fixed_header

	if mqtt.FixedHeader.Length != uint32(len(buf)) {
		log.Panicf("fixed header length(%d) not equal to acutall buf length(%d)\n", mqtt.FixedHeader.Length, len(buf))
	}

	msgType := mqtt.FixedHeader.MessageType
	if msgType <= 0 || msgType >= 15 {
		log.Panicf("MessageType(%d) in fixed header not supported\n", msgType)
	}

	switch msgType {
	case CONNECT:{
		mqtt.ProtocolName = getString(buf, &idx)
		mqtt.ProtocolVersion = getUint8(buf, &idx)
		mqtt.ConnectFlags = getConnectFlags(buf, &idx)
		mqtt.KeepAliveTimer = getUint16(buf, &idx)
		mqtt.ClientId = getString(buf, &idx)
		if mqtt.ConnectFlags.WillFlag{
			mqtt.WillTopic = getString(buf, &idx)
			mqtt.WillMessage = getString(buf, &idx)
		}
		if mqtt.ConnectFlags.UsernameFlag && idx < len(buf){
			mqtt.Username = getString(buf, &idx)
		}
		if mqtt.ConnectFlags.PasswordFlag && idx < len(buf){
			mqtt.Password = getString(buf, &idx)
		}
	}
	case CONNACK:{
		idx += 1
		mqtt.ReturnCode = uint8(getUint8(buf, &idx))
		if code := uint8(mqtt.ReturnCode);code > 5{
			return nil, errors.New("ReturnCode is invalid!")
		}
	}
	case PUBLISH:{
		mqtt.TopicName = getString(buf, &idx)
		if qos := mqtt.FixedHeader.QosLevel;qos == 1 || qos == 2{
			mqtt.MessageId = getUint16(buf, &idx)
		}
		mqtt.Data = buf[idx:len(buf)]
		idx = len(buf)
	}
	case PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK:{
		mqtt.MessageId = getUint16(buf, &idx)
	}
	case SUBSCRIBE:{
		if qos := mqtt.FixedHeader.QosLevel;qos == 1 || qos == 2{
			mqtt.MessageId = getUint16(buf, &idx)
		}
		topics := make([]string, 0)
		topics_qos := make([]uint8, 0)
		for ; idx < len(buf);{
			topics = append(topics, getString(buf, &idx))
			topics_qos = append(topics_qos, getUint8(buf, &idx))
		}          
		mqtt.Topics = topics
		mqtt.Topics_qos = topics_qos
	}
	case SUBACK:{
		mqtt.MessageId = getUint16(buf, &idx)
		topics_qos := make([]uint8, 0)
		for ; idx < len(buf);{
			topics_qos = append(topics_qos, getUint8(buf, &idx))
		}
		mqtt.Topics_qos = topics_qos
	}
	case UNSUBSCRIBE:{
		if qos := mqtt.FixedHeader.QosLevel;qos == 1 || qos == 2{
			mqtt.MessageId = getUint16(buf, &idx)
		}
		topics := make([]string, 0)
		for ; idx < len(buf);{
			topics = append(topics, getString(buf, &idx))
		}
		mqtt.Topics = topics
	}
	case PINGREQ: {
		// Nothing to do there
        // Here is one of the spots go-mode.el will
        // break in 'go-toto-beginning-of-line'
		
	}
	case PINGRESP: {
		// Nothing to do there
		
	}
	case DISCONNECT: {
		// Nothing to do there
	}
	}

	return mqtt, nil
}

func Decode(buf []byte)(*Mqtt, error){
    inx := 0
    fixed_header := getHeader(buf, &inx)
	return DecodeAfterFixedHeader(fixed_header, buf[inx:])
}

func setUint8(val uint8, buf *bytes.Buffer){
    buf.WriteByte(byte(val))
}

func setUint16(val uint16, buf *bytes.Buffer){
    buf.WriteByte(byte(val & 0xff00 >> 8))
    buf.WriteByte(byte(val & 0x00ff))
}

func setString(val string, buf *bytes.Buffer){
    length := uint16(len(val))
    setUint16(length, buf)
    buf.WriteString(val)
}

func setHeader(header *FixedHeader, buf *bytes.Buffer){
    val := byte(uint8(header.MessageType)) << 4
    val |= (boolToByte(header.DupFlag) << 3)
    val |= byte(header.QosLevel) << 1
    val |= boolToByte(header.Retain)
    buf.WriteByte(val)
}

func setConnectFlags(flags *ConnectFlags, buf *bytes.Buffer){
    val := boolToByte(flags.UsernameFlag) << 7
    val |= boolToByte(flags.PasswordFlag) << 6
    val |= boolToByte(flags.WillRetain) << 5
    val |= byte(flags.WillQos) << 3
    val |= boolToByte(flags.WillFlag) << 2
    val |= boolToByte(flags.CleanSession) << 1
    buf.WriteByte(val)
}

func boolToByte(val bool)byte{
    if val{
        return byte(1)
    }
    return byte(0)
}

func CreateMqtt(msg_type uint8) *Mqtt {
	mqtt := new(Mqtt)

	fixed_header := new(FixedHeader)
	fixed_header.MessageType = msg_type
	mqtt.FixedHeader = fixed_header

	connect_flags := new(ConnectFlags)
	mqtt.ConnectFlags = connect_flags

	switch msg_type {
		
	case CONNACK: {}
	case SUBACK: {}
	case UNSUBACK: {}
	case PINGRESP: {}

	default: {
		log.Panicf("Can't create Mqtt of type:%d", msg_type)
		return nil
	}	
	}

	return mqtt
}

func Encode(mqtt *Mqtt)([]byte, error){
    err := valid(mqtt)
    if err != nil{
        return nil, err
    }
    var headerbuf, buf bytes.Buffer
    setHeader(mqtt.FixedHeader, &headerbuf)
    switch mqtt.FixedHeader.MessageType{
	case CONNECT:{
		setString(mqtt.ProtocolName, &buf)
		setUint8(mqtt.ProtocolVersion, &buf)
		setConnectFlags(mqtt.ConnectFlags, &buf)
		setUint16(mqtt.KeepAliveTimer, &buf)
		setString(mqtt.ClientId, &buf)
		if mqtt.ConnectFlags.WillFlag{
			setString(mqtt.WillTopic, &buf)
			setString(mqtt.WillMessage, &buf)
		}
		if mqtt.ConnectFlags.UsernameFlag && len(mqtt.Username) > 0{
			setString(mqtt.Username, &buf)
		}
		if mqtt.ConnectFlags.PasswordFlag && len(mqtt.Password) > 0{
			setString(mqtt.Password, &buf)
		}
	}
	case CONNACK:{
		buf.WriteByte(byte(0))
		setUint8(uint8(mqtt.ReturnCode), &buf)
	}
	case PUBLISH:{
		setString(mqtt.TopicName, &buf)
		if qos := mqtt.FixedHeader.QosLevel;qos == 1 || qos == 2{
			setUint16(mqtt.MessageId, &buf)
		}
		buf.Write(mqtt.Data)
	}
	case PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK:{
		setUint16(mqtt.MessageId, &buf)
	}
	case SUBSCRIBE:{
		if qos := mqtt.FixedHeader.QosLevel;qos == 1 || qos == 2{
			setUint16(mqtt.MessageId, &buf)
		}
		for i := 0;i < len(mqtt.Topics);i += 1{
			setString(mqtt.Topics[i], &buf)
			setUint8(mqtt.Topics_qos[i], &buf)
		}
	}
	case SUBACK:{
		setUint16(mqtt.MessageId, &buf)
		for i := 0;i < len(mqtt.Topics_qos);i += 1{
			setUint8(mqtt.Topics_qos[i], &buf)
		}
	}
	case UNSUBSCRIBE:{
		if qos := mqtt.FixedHeader.QosLevel;qos == 1 || qos == 2{
			setUint16(mqtt.MessageId, &buf)
		}
		for i := 0;i < len(mqtt.Topics); i += 1{
			setString(mqtt.Topics[i], &buf)
		}
	}
    }
    if buf.Len() > 268435455{
        return nil, errors.New("Message is too long!")
    }
    encodeLength(uint32(buf.Len()), &headerbuf)
    headerbuf.Write(buf.Bytes())
    return headerbuf.Bytes(), nil
}

func valid(mqtt *Mqtt)error{
    if msgType := uint8(mqtt.FixedHeader.MessageType);msgType < 1 || msgType > 14{
        return errors.New("MessageType is invalid!")
    }
    if mqtt.FixedHeader.QosLevel > 3 {
        return errors.New("Qos Level is invalid!")
    }
    if mqtt.ConnectFlags != nil && mqtt.ConnectFlags.WillQos > 3{
        return errors.New("Will Qos Level is invalid!")
    }
    return nil
}

func decodeLength(b []byte, p *int)uint32{
    m := uint32(1)
    v := uint32(b[*p] & 0x7f)
    *p += 1
    for ; b[*p-1] & 0x80 > 0 ;{
        m *= 128
        v += uint32(b[*p] & 0x7f) * m
        *p += 1
    }
    return v
}

func encodeLength(length uint32, buf *bytes.Buffer){
    if length == 0{
        buf.WriteByte(byte(0))
        return
    }
    var lbuf bytes.Buffer
    for ; length > 0;{
        digit := length % 128
        length = length / 128
        if length > 0{
            digit = digit | 0x80
        }
        lbuf.WriteByte(byte(digit))
    }
    blen := lbuf.Bytes()
    for i := 1;i <= len(blen);i += 1{
        buf.WriteByte(blen[len(blen)-i])
    }
}