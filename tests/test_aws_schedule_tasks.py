import aws_schedule_tasks


def test_create_input_str_works():
    s = aws_schedule_tasks.create_input_str('foo', 'boop', True)
    assert 'foo' in s
    assert 'boop' in s
    assert 'yup' in s

    s = aws_schedule_tasks.create_input_str('foo', 'boop', False)
    assert 'yup' not in s
