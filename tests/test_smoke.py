"""骨架冒烟测试：所有模块可导入，类型可构造。"""


def test_imports():
    import app.api
    import app.config
    import app.errors
    import app.models
    import app.retry
    import app.scheduler
    import app.service
    import app.store
    import app.validation  # noqa: F401
    from app.handlers.builtin import BuiltinRegistry  # noqa: F401
    from app.models import TaskDef, TaskState, WorkflowDef

    wf = WorkflowDef(
        idempotency_key="k1",
        tasks=[TaskDef(name="a", handler="success")],
    )
    assert wf.tasks[0].name == "a"
    assert TaskState.PENDING.value == "PENDING"
