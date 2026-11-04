from s3mover.dates import find_key_date, parse_segment


def test_key_dates_in_common_formats():
    assert find_key_date("1KB/2025/11/2/x.bin") == (2025, 11, 2)
    assert find_key_date("EPCIS/2025-08-15/doc.xml") == (2025, 8, 15)
    assert find_key_date("EPCIS/20250815/doc.xml") == (2025, 8, 15)
    assert find_key_date("inbound-files/15-08-2025/a.xml") == (2025, 8, 15)
    assert find_key_date("inbound-files/report_2025_08_15.xml") == (2025, 8, 15)


def test_files_without_dates_are_not_misread():
    assert find_key_date("EPCIS/urn-epc-id-sscc-0614141.1234567890.xml") is None
    assert find_key_date("inbound-files/0012345678901.xml") is None   # long id, not a date
    assert find_key_date("EPCIS/20251399.xml") is None                 # month 13
    assert find_key_date("CHINA/x.xml") is None


def test_folder_segments_for_pruning():
    assert parse_segment((), "2025") == (2025,)
    assert parse_segment((2025,), "11") == (2025, 11)
    assert parse_segment((2025, 11), "2") == (2025, 11, 2)
    assert parse_segment((), "2025-08") == (2025, 8)
    assert parse_segment((), "202508") == (2025, 8)
    assert parse_segment((), "2025-08-15") == (2025, 8, 15)
    assert parse_segment((), "20250815") == (2025, 8, 15)
    assert parse_segment((2025, 8), "2025-08-15") == (2025, 8, 15)
    assert parse_segment((2025, 8), "2025-09-01") is None              # inconsistent with parent
    assert parse_segment((), "EPCIS") is None and parse_segment((), "1KB") is None
    assert parse_segment((2025, 2), "30") is None
