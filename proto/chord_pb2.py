# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: proto/chord.proto
# Protobuf Python Version: 5.26.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x11proto/chord.proto\x12\x05\x63hord\"0\n\x08NodeInfo\x12\n\n\x02ip\x18\x01 \x01(\t\x12\n\n\x02id\x18\x02 \x01(\x05\x12\x0c\n\x04port\x18\x03 \x01(\t\"<\n\x10\x46ingerTableEntry\x12\t\n\x01i\x18\x01 \x01(\x05\x12\x1d\n\x04node\x18\x02 \x01(\x0b\x32\x0f.chord.NodeInfo\"7\n\x0b\x46ingerTable\x12(\n\x07\x65ntries\x18\x01 \x03(\x0b\x32\x17.chord.FingerTableEntry\"<\n\x15SuccessorListResponse\x12#\n\nsuccessors\x18\x01 \x03(\x0b\x32\x0f.chord.NodeInfo\"\"\n\x14\x46indSuccessorRequest\x12\n\n\x02id\x18\x01 \x01(\x05\"\x07\n\x05\x45mpty2\xec\x02\n\x0c\x43hordService\x12/\n\x0cGetSuccessor\x12\x0c.chord.Empty\x1a\x0f.chord.NodeInfo\"\x00\x12\x43\n\x10GetSuccessorList\x12\x0f.chord.NodeInfo\x1a\x1c.chord.SuccessorListResponse\"\x00\x12\x34\n\x0eGetPredecessor\x12\x0f.chord.NodeInfo\x1a\x0f.chord.NodeInfo\"\x00\x12?\n\rFindSuccessor\x12\x1b.chord.FindSuccessorRequest\x1a\x0f.chord.NodeInfo\"\x00\x12\x35\n\x0f\x46indPredecessor\x12\x0f.chord.NodeInfo\x1a\x0f.chord.NodeInfo\"\x00\x12\x38\n\x0fInitFingerTable\x12\x0f.chord.NodeInfo\x1a\x12.chord.FingerTable\"\x00\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.chord_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_NODEINFO']._serialized_start=28
  _globals['_NODEINFO']._serialized_end=76
  _globals['_FINGERTABLEENTRY']._serialized_start=78
  _globals['_FINGERTABLEENTRY']._serialized_end=138
  _globals['_FINGERTABLE']._serialized_start=140
  _globals['_FINGERTABLE']._serialized_end=195
  _globals['_SUCCESSORLISTRESPONSE']._serialized_start=197
  _globals['_SUCCESSORLISTRESPONSE']._serialized_end=257
  _globals['_FINDSUCCESSORREQUEST']._serialized_start=259
  _globals['_FINDSUCCESSORREQUEST']._serialized_end=293
  _globals['_EMPTY']._serialized_start=295
  _globals['_EMPTY']._serialized_end=302
  _globals['_CHORDSERVICE']._serialized_start=305
  _globals['_CHORDSERVICE']._serialized_end=669
# @@protoc_insertion_point(module_scope)