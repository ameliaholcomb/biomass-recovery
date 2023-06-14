import pytest

from biomassrecovery.data.gedi_database import gedi_sql_query, Like, RegEx

def test_simple_query():
	query = gedi_sql_query(
		table_name="level_4a",
		columns=["agbd"],
		limit=10
	)
	assert query == "SELECT agbd FROM level_4a LIMIT 10"

def test_unfiltered_no_force():
	with pytest.raises(UserWarning):
		_ = gedi_sql_query("level_4a")

def test_wildcard_with_force():
	query = gedi_sql_query("level_4a", force=True)
	assert query == "SELECT * FROM level_4a"

def test_order_by_ascending():
	query = gedi_sql_query(
		"level_4a",
		force=True,
		order_by=["agbd"],
	)
	assert query == "SELECT * FROM level_4a ORDER BY agbd"

def test_order_by_descending():
	query = gedi_sql_query(
		"level_4a",
		force=True,
		order_by=["-agbd"],
	)
	assert query == "SELECT * FROM level_4a ORDER BY agbd DESC"

def test_add_one_filter_int():
	query = gedi_sql_query(
		"level_4a",
		quality=1
	)
	assert query == "SELECT * FROM level_4a WHERE (quality = 1)"

def test_add_one_filter_str():
	query = gedi_sql_query(
		"level_4a",
		beam_type="full"
	)
	assert query == "SELECT * FROM level_4a WHERE (beam_type = 'full')"

def test_add_multiple_filter():
	query = gedi_sql_query(
		"level_4a",
		quality=1,
		beam_type="full"
	)
	assert query == "SELECT * FROM level_4a WHERE (quality = 1) AND (beam_type = 'full')"

def test_like_predicate():
	query = gedi_sql_query(
		"level_4a",
		quality=1,
		beam_type=Like("ful")
	)
	assert query == "SELECT * FROM level_4a WHERE (quality = 1) AND (beam_type LIKE 'ful')"

def test_regex_predicate():
	query = gedi_sql_query(
		"level_4a",
		quality=1,
		beam_type=RegEx("f.*")
	)
	assert query == "SELECT * FROM level_4a WHERE (quality = 1) AND (beam_type ~ 'f.*')"
