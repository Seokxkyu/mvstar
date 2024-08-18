from mvstar.movie import get_data

def test_get_data():
    assert not get_data(2015)
    assert not get_data(2016)
    assert not get_data(year=2017, sleep_time=0.1)
    assert get_data(year=2018, sleep_time=0.1)
    assert get_data(year=2019, sleep_time=0.1)
    assert get_data(year=2020, sleep_time=0.1)
