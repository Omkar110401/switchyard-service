import sys
sys.path.insert(0, '.')

from shared.dag import validate_workflow, DAGValidator, ValidationError


def test_linear_chain():
    """Test: simple linear workflow (no cycles)."""
    workflow = {
        'name': 'linear',
        'tasks': [
            {'id': 'a', 'command': 'a.py'},
            {'id': 'b', 'depends_on': ['a'], 'command': 'b.py'},
            {'id': 'c', 'depends_on': ['b'], 'command': 'c.py'},
        ]
    }
    result = validate_workflow(workflow)
    assert result.is_valid, f"Expected valid, got errors: {result.errors}"
    assert result.topological_order == ['a', 'b', 'c'], f"Wrong order: {result.topological_order}"
    print("✓ test_linear_chain passed")


def test_cycle_detection():
    """Test: detect cycle in workflow."""
    workflow = {
        'name': 'cycle',
        'tasks': [
            {'id': 'a', 'depends_on': ['b'], 'command': 'a.py'},
            {'id': 'b', 'depends_on': ['a'], 'command': 'b.py'},
        ]
    }
    result = validate_workflow(workflow)
    assert not result.is_valid, "Expected invalid due to cycle"
    assert any(e[0] == ValidationError.CYCLE_DETECTED for e in result.errors)
    print("✓ test_cycle_detection passed")


def test_self_loop():
    """Test: detect task depending on itself."""
    workflow = {
        'name': 'self_loop',
        'tasks': [
            {'id': 'a', 'depends_on': ['a'], 'command': 'a.py'},
        ]
    }
    result = validate_workflow(workflow)
    assert not result.is_valid, "Expected invalid due to self-loop"
    assert any(e[0] == ValidationError.SELF_LOOP for e in result.errors)
    print("✓ test_self_loop passed")


def test_parallel_branches():
    """Test: parallel branches with fan-out and fan-in."""
    workflow = {
        'name': 'parallel',
        'tasks': [
            {'id': 'fetch', 'command': 'fetch.py'},
            {'id': 'process_1', 'depends_on': ['fetch'], 'command': 'p1.py'},
            {'id': 'process_2', 'depends_on': ['fetch'], 'command': 'p2.py'},
            {'id': 'merge', 'depends_on': ['process_1', 'process_2'], 'command': 'merge.py'},
        ]
    }
    result = validate_workflow(workflow)
    assert result.is_valid, f"Expected valid, got errors: {result.errors}"
    assert result.topological_order[0] == 'fetch'
    assert 'process_1' in result.topological_order
    assert 'process_2' in result.topological_order
    assert result.topological_order[-1] == 'merge'
    print("✓ test_parallel_branches passed")


def test_missing_dependency():
    """Test: detect missing dependency."""
    workflow = {
        'name': 'missing_dep',
        'tasks': [
            {'id': 'a', 'depends_on': ['nonexistent'], 'command': 'a.py'},
        ]
    }
    result = validate_workflow(workflow)
    assert not result.is_valid
    assert any(e[0] == ValidationError.MISSING_DEPENDENCY for e in result.errors)
    print("✓ test_missing_dependency passed")


def test_get_ready_tasks():
    """Test: get_ready_tasks returns correct tasks."""
    workflow = {
        'name': 'test',
        'tasks': [
            {'id': 'a', 'command': 'a.py'},
            {'id': 'b', 'depends_on': ['a'], 'command': 'b.py'},
            {'id': 'c', 'depends_on': ['b'], 'command': 'c.py'},
        ]
    }
    validator = DAGValidator(workflow)

    ready = validator.get_ready_tasks(set())
    assert ready == ['a'], f"With no completed, expected ['a'], got {ready}"

    ready = validator.get_ready_tasks({'a'})
    assert ready == ['b'], f"With 'a' done, expected ['b'], got {ready}"

    ready = validator.get_ready_tasks({'a', 'b'})
    assert ready == ['c'], f"With 'a','b' done, expected ['c'], got {ready}"

    ready = validator.get_ready_tasks({'a', 'b', 'c'})
    assert ready == [], f"With all done, expected [], got {ready}"

    print("✓ test_get_ready_tasks passed")


def test_get_dependencies():
    """Test: get_dependencies returns correct list."""
    workflow = {
        'name': 'test',
        'tasks': [
            {'id': 'a', 'command': 'a.py'},
            {'id': 'b', 'depends_on': ['a'], 'command': 'b.py'},
            {'id': 'c', 'depends_on': ['a', 'b'], 'command': 'c.py'},
        ]
    }
    validator = DAGValidator(workflow)

    assert validator.get_dependencies('a') == []
    assert validator.get_dependencies('b') == ['a']
    assert set(validator.get_dependencies('c')) == {'a', 'b'}
    print("✓ test_get_dependencies passed")


def test_get_dependents():
    """Test: get_dependents returns correct list."""
    workflow = {
        'name': 'test',
        'tasks': [
            {'id': 'a', 'command': 'a.py'},
            {'id': 'b', 'depends_on': ['a'], 'command': 'b.py'},
            {'id': 'c', 'depends_on': ['a'], 'command': 'c.py'},
        ]
    }
    validator = DAGValidator(workflow)

    assert set(validator.get_dependents('a')) == {'b', 'c'}
    assert validator.get_dependents('b') == []
    assert validator.get_dependents('c') == []
    print("✓ test_get_dependents passed")


def test_depends_on_string():
    """Test: depends_on can be string or list."""
    workflow_str = {
        'name': 'test_str',
        'tasks': [
            {'id': 'a', 'command': 'a.py'},
            {'id': 'b', 'depends_on': 'a', 'command': 'b.py'},  # string, not list
        ]
    }
    result = validate_workflow(workflow_str)
    assert result.is_valid, f"Expected valid, got errors: {result.errors}"
    print("✓ test_depends_on_string passed")


if __name__ == '__main__':
    test_linear_chain()
    test_cycle_detection()
    test_self_loop()
    test_parallel_branches()
    test_missing_dependency()
    test_get_ready_tasks()
    test_get_dependencies()
    test_get_dependents()
    test_depends_on_string()
    print("\n✅ All tests passed!")
