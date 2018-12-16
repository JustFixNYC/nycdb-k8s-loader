import aws_schedule_tasks


def test_sanity_check_works():
    aws_schedule_tasks.sanity_check()


def test_create_input_str_works():
    s = aws_schedule_tasks.create_input_str('boop', True)
    assert 'boop' in s
    assert 'yup' in s

    s = aws_schedule_tasks.create_input_str('boop', False)
    assert 'yup' not in s
