from mdbl.mdbl import ValidFileTypes, read_mapping


def test_parse_toml():
    file = open("test.toml", "rb")
    file_type = ValidFileTypes.TOML

    mapping = read_mapping(file, file_type)

    assert mapping["object"].step == 0
    assert mapping["detections"].step == 1
