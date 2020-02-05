package model

type JsonToProto interface {
    convertToProto() ProtoToJson
}

type ProtoToJson interface {
    convertToJson() JsonToProto
}