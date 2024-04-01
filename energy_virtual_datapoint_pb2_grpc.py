# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import energy_virtual_datapoint_pb2 as energy__virtual__datapoint__pb2
from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2


class EnergyVirtualDatapointStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Create = channel.unary_unary(
                '/sbos.energy.EnergyVirtualDatapoint/Create',
                request_serializer=energy__virtual__datapoint__pb2.VirtualDatapoint.SerializeToString,
                response_deserializer=energy__virtual__datapoint__pb2.CreateVirtualDatapointResponse.FromString,
                )
        self.Update = channel.unary_unary(
                '/sbos.energy.EnergyVirtualDatapoint/Update',
                request_serializer=energy__virtual__datapoint__pb2.VirtualDatapoint.SerializeToString,
                response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
                )
        self.QueryVirtualDatapoint = channel.unary_stream(
                '/sbos.energy.EnergyVirtualDatapoint/QueryVirtualDatapoint',
                request_serializer=energy__virtual__datapoint__pb2.QueryVirtualDatapointRequest.SerializeToString,
                response_deserializer=energy__virtual__datapoint__pb2.VirtualDatapoint.FromString,
                )
        self.Get = channel.unary_unary(
                '/sbos.energy.EnergyVirtualDatapoint/Get',
                request_serializer=energy__virtual__datapoint__pb2.GetVirtualDatapointRequest.SerializeToString,
                response_deserializer=energy__virtual__datapoint__pb2.VirtualDatapoint.FromString,
                )
        self.TestFormula = channel.unary_unary(
                '/sbos.energy.EnergyVirtualDatapoint/TestFormula',
                request_serializer=energy__virtual__datapoint__pb2.TestFormulaRequest.SerializeToString,
                response_deserializer=energy__virtual__datapoint__pb2.TestFormulaResponse.FromString,
                )
        self.Delete = channel.unary_unary(
                '/sbos.energy.EnergyVirtualDatapoint/Delete',
                request_serializer=energy__virtual__datapoint__pb2.DeleteVirtualDatapointRequest.SerializeToString,
                response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
                )


class EnergyVirtualDatapointServicer(object):
    """Missing associated documentation comment in .proto file."""

    def Create(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Update(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def QueryVirtualDatapoint(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Get(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def TestFormula(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Delete(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_EnergyVirtualDatapointServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Create': grpc.unary_unary_rpc_method_handler(
                    servicer.Create,
                    request_deserializer=energy__virtual__datapoint__pb2.VirtualDatapoint.FromString,
                    response_serializer=energy__virtual__datapoint__pb2.CreateVirtualDatapointResponse.SerializeToString,
            ),
            'Update': grpc.unary_unary_rpc_method_handler(
                    servicer.Update,
                    request_deserializer=energy__virtual__datapoint__pb2.VirtualDatapoint.FromString,
                    response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            ),
            'QueryVirtualDatapoint': grpc.unary_stream_rpc_method_handler(
                    servicer.QueryVirtualDatapoint,
                    request_deserializer=energy__virtual__datapoint__pb2.QueryVirtualDatapointRequest.FromString,
                    response_serializer=energy__virtual__datapoint__pb2.VirtualDatapoint.SerializeToString,
            ),
            'Get': grpc.unary_unary_rpc_method_handler(
                    servicer.Get,
                    request_deserializer=energy__virtual__datapoint__pb2.GetVirtualDatapointRequest.FromString,
                    response_serializer=energy__virtual__datapoint__pb2.VirtualDatapoint.SerializeToString,
            ),
            'TestFormula': grpc.unary_unary_rpc_method_handler(
                    servicer.TestFormula,
                    request_deserializer=energy__virtual__datapoint__pb2.TestFormulaRequest.FromString,
                    response_serializer=energy__virtual__datapoint__pb2.TestFormulaResponse.SerializeToString,
            ),
            'Delete': grpc.unary_unary_rpc_method_handler(
                    servicer.Delete,
                    request_deserializer=energy__virtual__datapoint__pb2.DeleteVirtualDatapointRequest.FromString,
                    response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'sbos.energy.EnergyVirtualDatapoint', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class EnergyVirtualDatapoint(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def Create(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/sbos.energy.EnergyVirtualDatapoint/Create',
            energy__virtual__datapoint__pb2.VirtualDatapoint.SerializeToString,
            energy__virtual__datapoint__pb2.CreateVirtualDatapointResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Update(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/sbos.energy.EnergyVirtualDatapoint/Update',
            energy__virtual__datapoint__pb2.VirtualDatapoint.SerializeToString,
            google_dot_protobuf_dot_empty__pb2.Empty.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def QueryVirtualDatapoint(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/sbos.energy.EnergyVirtualDatapoint/QueryVirtualDatapoint',
            energy__virtual__datapoint__pb2.QueryVirtualDatapointRequest.SerializeToString,
            energy__virtual__datapoint__pb2.VirtualDatapoint.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Get(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/sbos.energy.EnergyVirtualDatapoint/Get',
            energy__virtual__datapoint__pb2.GetVirtualDatapointRequest.SerializeToString,
            energy__virtual__datapoint__pb2.VirtualDatapoint.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def TestFormula(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/sbos.energy.EnergyVirtualDatapoint/TestFormula',
            energy__virtual__datapoint__pb2.TestFormulaRequest.SerializeToString,
            energy__virtual__datapoint__pb2.TestFormulaResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def Delete(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/sbos.energy.EnergyVirtualDatapoint/Delete',
            energy__virtual__datapoint__pb2.DeleteVirtualDatapointRequest.SerializeToString,
            google_dot_protobuf_dot_empty__pb2.Empty.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)