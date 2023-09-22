package proto

import (
	"github.com/jhump/protoreflect/desc/protoparse"
	"go.k6.io/k6/js/modules"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

func init() {
	modules.Register("k6/x/proto", New())
}

type (
	RootModule struct{}

	Proto struct {
		vu modules.VU
		*Protobuf
	}
)

var (
	_ modules.Instance = &Proto{}
	_ modules.Module   = &RootModule{}
)

func New() *RootModule {
	return &RootModule{}
}

func (*RootModule) NewModuleInstance(vu modules.VU) modules.Instance {
	return &Proto{vu: vu, Protobuf: &Protobuf{vu: vu}}
}

type Protobuf struct {
	vu modules.VU
}

type ProtoFile struct {
	messageDescriptor protoreflect.MessageDescriptor
}

func (p *Protobuf) Load(filePath, messageType string) (ProtoFile, error) {
	parser := protoparse.Parser{}
	fileDesc, err := parser.ParseFiles(filePath)
	if err != nil {
		return ProtoFile{}, err
	}
	scm := fileDesc[0].AsFileDescriptorProto()
	fd, err := protodesc.NewFile(scm, nil)
	if err != nil {
		return ProtoFile{},err
	}
	return ProtoFile{fd.Messages().ByName(protoreflect.Name(messageType))},nil
}


func (pf *ProtoFile) Encode(data string) (encoded []byte, err error) {
	msg := dynamicpb.NewMessage(pf.messageDescriptor)
	err = protojson.Unmarshal([]byte(data), msg)
	if err != nil {
		return encoded, err
	}
	encoded, err = proto.Marshal(msg)
	if err != nil {
		return encoded, err
	}
	return encoded, nil
}

func (pf *ProtoFile) Decode(data []byte) (string, error) {
	msg := dynamicpb.NewMessage(pf.messageDescriptor)
	err := proto.Unmarshal(data, msg)
	if err != nil {
		return "", err
	}
	marshalOptions := protojson.MarshalOptions{
		UseProtoNames: true,
	}
	decoded, err := marshalOptions.Marshal(msg)
	if err != nil {
		return "", err
	}
	return string(decoded), nil
}


func (p *Proto) Exports() modules.Exports {
	return modules.Exports{Default: p.Protobuf}
}
