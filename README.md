# Intro_Spark

### Pytest 工作流程

           你在命令行执行 pytest
                       │
                       ▼
            ┌──────────────────────┐
            │   测试发现 (discovery)│
            │ 扫描 test_*.py 文件   │
            └──────────────────────┘
                       │
                       ▼
            ┌─────────────────────┐
            │   conftest.py 加载   │
            │ fixture / 配置生效   │
            └─────────────────────┘
                       │
                       ▼
            ┌───────────────────────┐
            │   测试函数收集          │
            │ e.g. test_load(spark) │
            └───────────────────────┘
                       │
                       ▼
            ┌─────────────────────┐
            │ fixture 依赖注入     │
            │ pytest 找到 spark() │
            └─────────────────────┘
                       │
                       ▼
            ┌─────────────────────┐
            │   运行测试函数        │
            │ assert 表达式检查     │
            └─────────────────────┘
                       │
                       ▼
            ┌─────────────────────┐
            │   生成测试报告        │
            └─────────────────────┘

```bash
pytest -q
```

## 🔑 总结

- create a folder named "**tests**", inside creating files: conftest.py and test_*.py
- **pytest 自动发现** → 找到 test 文件和函数
- **conftest.py 自动加载** → 提供共享的 fixture
- **fixture 注入** → 根据函数参数名自动匹配并传入对象

  - @pytest.fixture(scope="session")
  - `@pytest.fixture` 定义一个测试资源。

    `scope="session"` → **整个测试会话中只创建一次**，通常用于比较重的资源
- **assert 简洁检查** → 无需 unittest 那种冗长 API
