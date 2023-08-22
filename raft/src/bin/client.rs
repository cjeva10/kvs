use raft::RaftClient;
use raft::RequestVoteArgs;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = RaftClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(RequestVoteArgs {
        term: 5,
        candidate_id: 2,
        last_log_index: 1,
        last_log_term: 2,
    });

    let response = client.request_vote(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}
