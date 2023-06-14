import numpy as np
import pytest

from biomassrecovery.processing.jrc_processing import last_of_value, first_of_value

@pytest.mark.parametrize(
	"input,value,expected",
	[
		(
			[[1, 2, 3], [2, 5, 6], [1, 2, 2]],
			2,
			[1, 2, 2],
		),
		(
			[[1, 2, 3], [4, 5, 6], [0, 1, 2]],
			2,
			[-9999, 0, 2],
		),
		(
			[[1, 2, 3], [4, 5, 6], [0, 1, 2]],
			42,
			[-9999, -9999, -9999],
		)
	]
)
def test_last_of_value(input, value, expected):
	narray = np.array(input)
	result = last_of_value(narray, value)
	assert (result == np.array(expected)).all()

@pytest.mark.parametrize(
	"input,value,expected",
	[
		(
			[[1, 2, 3], [2, 5, 6], [1, 2, 2]],
			2,
			[1, 0, 2],
		),
		(
			[[1, 2, 3], [4, 5, 6], [0, 1, 2]],
			2,
			[-9999, 0, 2],
		),
		(
			[[1, 2, 3], [4, 5, 6], [0, 1, 2]],
			42,
			[-9999, -9999, -9999],
		)
	]
)
def test_first_of_value(input, value, expected):
	narray = np.array(input)
	result = first_of_value(narray, value)
	assert (result == np.array(expected)).all()