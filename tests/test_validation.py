import pytest

from de_challenge.domain.validation import validate_coordinates


def test_validate_coordinates_accepts_valid_range():
    validate_coordinates(14.0, 50.0)


def test_validate_coordinates_rejects_invalid_lon():
    with pytest.raises(ValueError):
        validate_coordinates(190.0, 45.0)


def test_validate_coordinates_rejects_invalid_lat():
    with pytest.raises(ValueError):
        validate_coordinates(14.0, -95.0)
