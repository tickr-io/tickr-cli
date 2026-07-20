use crate::proto::executor_client::ExecutorClient;
use tonic::transport::Channel;

pub type GrpcClient = ExecutorClient<Channel>;
