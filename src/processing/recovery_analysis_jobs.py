import argparse
import logging
import pandas as pd
import pathlib
import re
from typing import List, Tuple
from tqdm.autonotebook import tqdm

from src.constants import PAISAGENSLIDAR_PATH, GEDI_L2A_PATH, EBALIDAR_PATH, SCRATCH_PATH
from src.processing import recovery_analysis 
from src.utils.logging import get_logger


logger = get_logger(__name__)
logger.setLevel(logging.INFO)


EBA_PATH = SCRATCH_PATH / "EBA_lidar" / "laz_EBA_processed"

def _get_eba_files() -> List[pathlib.Path]:
	return [survey for survey in EBA_PATH.iterdir()
		if (survey / "grid_metrics").exists() and
			any((survey / "grid_metrics").iterdir())
	]

def read_checkpoint_data(
	save_path: pathlib.Path) -> Tuple[int, List[pd.DataFrame]]:

	start_idx = 0
	dfs = []
	logger.info("Recovering from saved state ...")
	saved_feathers = list(save_path.glob("*.feather"))
	for feather in saved_feathers:
		dfs.append(pd.read_feather(feather))
		pattern = re.compile(r"_([0-9]+)_")
		m = re.search(pattern, feather.name)
		if not m:
			raise RuntimeError(f"Unable to recover state: found badly formatted file {feather.name}")
		start_idx = max(start_idx, int(m.groups(0)[0]) + 1)
	return start_idx, dfs



def run_forest_recovery_analysis(
	surveys: List[pathlib.Path],
	save_every: int,
	save_path: pathlib.Path,
	read_checkpoints: bool):

	start_idx = 0
	all_dfs = []
	if read_checkpoints:
		start_idx, dfs = read_checkpoint_data(save_path)
		all_dfs.extend(dfs)
		print(f"Found {len(all_dfs)} recovery files, starting work at {start_idx}")


	num_records = 0
	intermediate_dfs = []
	for i, survey in enumerate(tqdm(surveys[start_idx:])):

		try:
			df = recovery_analysis.compute_recovery_dataset(survey.name)
			num_records += len(df)
			intermediate_dfs.append(df)
		except (KeyError, ValueError, RuntimeError) as e:
			logger.warning(
				"Encountered error in survey {}, skipping: {}".format(
					survey.name, e)
			)
		
		if num_records >= save_every:
			# Intermediate feather files are saved with the format
			# intermediate_{last survey idx}_{last survey name}_{num records in file}
			file_name = save_path / f"intermediate_{i + start_idx}_{survey.name}_{num_records}.feather"
			logger.info("Saving intermediate file {file_name} ...")
			intermediate = pd.concat(intermediate_dfs, ignore_index=True)
			intermediate.to_feather(file_name)
			all_dfs.extend(intermediate)
			num_records = 0
			intermediate_dfs = []
	data = pd.concat(all_dfs, ignore_index=True)
	data.to_feather(save_path / "all.feather")

if __name__ == "__main__":
	
	parser = argparse.ArgumentParser(description="Run recovery analysis jobs")
	parser.add_argument(
		"--dataset",
		help="Dataset on which to run recovery analysis."
			"Can be GEDI, Paisagenslidar, or EBA",
		type=str,
	)
	parser.add_argument(
		"--save_path", 
		help="Path in which to save intermediate and final results",
		type=str
	)
	parser.add_argument(
		"--save_every", 
		help="Save intermediate results every n records",
		type=int
	)
	parser.add_argument(
		"--read_checkpoints", 
		help="Whether or not to recover from checkpointed state",
		type=bool
	)
	args = parser.parse_args()
	if args.dataset.lower() == "eba":
		logger.info("Finding EBA surveys ...")
		surveys = _get_eba_files()
	else:
		raise ValueError("Unknown dataset option {}".format(args.dataset))

	save_dir = pathlib.Path(args.save_path)
	assert save_dir.exists()
	run_forest_recovery_analysis(surveys=surveys,
		save_every=args.save_every,
		save_path=pathlib.Path(args.save_path),
		read_checkpoints=args.read_checkpoints)
