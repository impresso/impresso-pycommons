import pytest
import requests
from impresso_commons.images.olive_boxes import get_iiif_url


test_data = [
    (
        "indeplux-1898-06-01-a-p0001",
        "0 0 800 800",
        None,
        "https://iiif.eluxemburgensia.lu/iiif/2/ark:%2f70795%2fxh85sz%2fpages%2f1/info.json",
        False,
        "https://iiif.eluxemburgensia.lu/iiif/2/ark:%2f70795%2fxh85sz%2fpages%2f1/0,0,800,800/full/0/default.jpg",
    ),
    (
        "GDL-1901-01-02-a-p0001",
        "0 0 800 800",
        None,
        None,
        False,
        "http://dhlabsrv17.epfl.ch/iiif_impresso/GDL-1901-01-02-a-p0001/0,0,800,800/full/0/default.jpg",
    ),
    (
        "GDL-1901-01-02-a-p0001",
        "62.126,23.868000000000002,11.312000000000005,0.558",
        None,
        "https://chroniclingamerica.loc.gov/iiif/2/mimtptc_bath_ver01%2Fdata%2Fsn92063852%2F00271764509%2F1950051301%2F0447.jp2",
        True,
        "https://chroniclingamerica.loc.gov/iiif/2/mimtptc_bath_ver01%2Fdata%2Fsn92063852%2F00271764509%2F1950051301%2F0447.jp2/pct:62.126,23.868000000000002,11.312000000000005,0.558/full/0/default.jpg",
    ),
]


@pytest.mark.parametrize("page_id, box, base_uri, iiif_manifest, percentage, expected_iiif_url", test_data)
def test_get_iiif_url(page_id, box, base_uri, iiif_manifest, percentage, expected_iiif_url):
    url = get_iiif_url(page_id, box, iiif_manifest_uri=iiif_manifest, pct=percentage)
    assert url == expected_iiif_url
