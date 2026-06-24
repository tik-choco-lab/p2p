use super::*;

#[tokio::test]
async fn add_serve_registers_forward() {
    let controller = ForwardController::new_inert();

    let outcome = execute_line(&controller, "add serve tcp://127.0.0.1:80")
        .await
        .unwrap();

    assert_eq!(outcome.output, "added tcp:80\n");
    assert!(!outcome.should_quit);
    let statuses = controller.list_forwards().await;
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].spec.direction, Direction::Serve);
    assert_eq!(statuses[0].spec.addr, "127.0.0.1:80");
}

#[tokio::test]
async fn add_connect_registers_listen_and_remote_target() {
    let controller = ForwardController::new_inert();

    execute_line(&controller, "add connect udp://19000:9000")
        .await
        .unwrap();

    let statuses = controller.list_forwards().await;
    assert_eq!(statuses[0].spec.direction, Direction::Connect);
    assert_eq!(statuses[0].spec.proto, Proto::Udp);
    assert_eq!(statuses[0].spec.listen_port, 19000);
    assert_eq!(statuses[0].spec.target, "udp:9000");
}

#[tokio::test]
async fn remove_deletes_forward() {
    let controller = ForwardController::new_inert();
    execute_line(&controller, "add serve :80").await.unwrap();

    let outcome = execute_line(&controller, "remove tcp:80").await.unwrap();

    assert_eq!(outcome.output, "removed tcp:80\n");
    assert!(controller.list_forwards().await.is_empty());
}

#[tokio::test]
async fn list_renders_registered_forwards() {
    let controller = ForwardController::new_inert();
    execute_line(&controller, "add serve :80").await.unwrap();

    let outcome = execute_line(&controller, "list").await.unwrap();

    assert!(outcome.output.contains("key direction proto endpoint"));
    assert!(outcome.output.contains("tcp:80 serve tcp :80 listening"));
}

#[tokio::test]
async fn quit_marks_shell_complete() {
    let controller = ForwardController::new_inert();

    let outcome = execute_line(&controller, "quit").await.unwrap();

    assert_eq!(outcome.output, "bye\n");
    assert!(outcome.should_quit);
}

#[tokio::test]
async fn unknown_command_is_rejected() {
    let controller = ForwardController::new_inert();

    let err = execute_line(&controller, "wat").await.unwrap_err();

    assert!(err.to_string().contains("unknown command"));
}
