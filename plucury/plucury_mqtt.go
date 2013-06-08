package parser

import ("bytes"
        "errors")

type MessageType uint8
type ReturnCode uint8
type Header struct{
    MessageType MessageType
    DupFlag, Retain bool
    QosLevel uint8
    Length uint32
}
type ConnectFlags struct{
    UsernameFlag, PasswordFlag, WillRetain, WillFlag, CleanSession bool
    WillQos uint8
}
type Mqtt struct{
    Header *Header
    ProtocolName, TopicName, ClientId, WillTopic, WillMessage, Username, Password string
    ProtocolVersion uint8
    ConnectFlags *ConnectFlags
    KeepAliveTimer, MessageId uint16
    Data []byte
    Topics []string
    Topics_qos []uint8
    ReturnCode ReturnCode
}

const(
    CONNECT = MessageType(iota + 1)
    CONNACK
    PUBLISH
    PUBACK
    PUBREC
    PUBREL
    PUBCOMP
    SUBSCRIBE
    SUBACK
    UNSUBSCRIBE
    UNSUBACK
    PINGREQ
    PINGRESP
    DISCONNECT
)

const(
    ACCEPTED = ReturnCode(iota)
    UNACCEPTABLE_PROTOCOL_VERSION
    IDENTIFIER_REJECTED
    SERVER_UNAVAILABLE
    BAD_USERNAME_OR_PASSWORD
    NOT_AUTHORIZED
)

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

func getHeader(b []byte, p *int)*Header{
    byte1 := b[*p]
    *p += 1
    header := new(Header)
    header.MessageType = MessageType(byte1 & 0xF0 >> 4)
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

func Decode(b []byte)(*Mqtt, error){
    mqtt := new(Mqtt)
    inx := 0
    mqtt.Header = getHeader(b, &inx)
    if mqtt.Header.Length != uint32(len(b) - inx){
        return nil, errors.New("Message length is wrong!")
    }
    if msgType := uint8(mqtt.Header.MessageType); msgType < 1 || msgType > 14{
        return nil, errors.New("Message Type is invalid!")
    }
    switch mqtt.Header.MessageType{
        case CONNECT:{
            mqtt.ProtocolName = getString(b, &inx)
            mqtt.ProtocolVersion = getUint8(b, &inx)
            mqtt.ConnectFlags = getConnectFlags(b, &inx)
            mqtt.KeepAliveTimer = getUint16(b, &inx)
            mqtt.ClientId = getString(b, &inx)
            if mqtt.ConnectFlags.WillFlag{
                mqtt.WillTopic = getString(b, &inx)
                mqtt.WillMessage = getString(b, &inx)
            }
            if mqtt.ConnectFlags.UsernameFlag && inx < len(b){
                mqtt.Username = getString(b, &inx)
            }
            if mqtt.ConnectFlags.PasswordFlag && inx < len(b){
                mqtt.Password = getString(b, &inx)
            }
        }
        case CONNACK:{
            inx += 1
            mqtt.ReturnCode = ReturnCode(getUint8(b, &inx))
            if code := uint8(mqtt.ReturnCode);code > 5{
                return nil, errors.New("ReturnCode is invalid!")
            }
        }
        case PUBLISH:{
            mqtt.TopicName = getString(b, &inx)
            if qos := mqtt.Header.QosLevel;qos == 1 || qos == 2{
                mqtt.MessageId = getUint16(b, &inx)
            }
            mqtt.Data = b[inx:len(b)]
            inx = len(b)
        }
        case PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK:{
            mqtt.MessageId = getUint16(b, &inx)
        }
        case SUBSCRIBE:{
            if qos := mqtt.Header.QosLevel;qos == 1 || qos == 2{
                mqtt.MessageId = getUint16(b, &inx)
            }
            topics := make([]string, 0)
            topics_qos := make([]uint8, 0)
            for ; inx < len(b);{
                topics = append(topics, getString(b, &inx))
                topics_qos = append(topics_qos, getUint8(b, &inx))
            }
            mqtt.Topics = topics
            mqtt.Topics_qos = topics_qos
        }
        case SUBACK:{
            mqtt.MessageId = getUint16(b, &inx)
            topics_qos := make([]uint8, 0)
            for ; inx < len(b);{
                topics_qos = append(topics_qos, getUint8(b, &inx))
            }
            mqtt.Topics_qos = topics_qos
        }
        case UNSUBSCRIBE:{
            if qos := mqtt.Header.QosLevel;qos == 1 || qos == 2{
                mqtt.MessageId = getUint16(b, &inx)
            }
            topics := make([]string, 0)
            for ; inx < len(b);{
                topics = append(topics, getString(b, &inx))
            }
            mqtt.Topics = topics
        }
    }
    return mqtt, nil
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

func setHeader(header *Header, buf *bytes.Buffer){
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

func Encode(mqtt *Mqtt)([]byte, error){
    err := valid(mqtt)
    if err != nil{
        return nil, err
    }
    var headerbuf, buf bytes.Buffer
    setHeader(mqtt.Header, &headerbuf)
    switch mqtt.Header.MessageType{
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
            if qos := mqtt.Header.QosLevel;qos == 1 || qos == 2{
                setUint16(mqtt.MessageId, &buf)
            }
            buf.Write(mqtt.Data)
        }
        case PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK:{
            setUint16(mqtt.MessageId, &buf)
        }
        case SUBSCRIBE:{
            if qos := mqtt.Header.QosLevel;qos == 1 || qos == 2{
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
            if qos := mqtt.Header.QosLevel;qos == 1 || qos == 2{
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
    if msgType := uint8(mqtt.Header.MessageType);msgType < 1 || msgType > 14{
        return errors.New("MessageType is invalid!")
    }
    if mqtt.Header.QosLevel > 3 {
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