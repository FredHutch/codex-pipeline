#!/usr/bin/env python3

import argparse
import json
import logging
import re
import sys
import yaml

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)-7s - %(message)s'
)
logger = logging.getLogger(__name__)

# Some constants to use below.
path_format = "keyence_multi_cycle_v01"
gpus = [ 0, 1 ]
memory_limit = "64G"



########
# MAIN #
########
if __name__ == "__main__" :
    # Set up argument parser and parse the command line arguments.
    parser = argparse.ArgumentParser(
        description = "Create a YAML config file for Cytokit, based on a JSON file from the CODEX Toolkit pipeline. YAML file will be created in current working directory unless otherwise specified."
    )
    parser.add_argument(
        "pipelineConfigFilename",
        help = "JSON file containing all information required for config generation."
    )
    parser.add_argument(
        "-o",
        "--outfile",
        help = "Path to output YAML config file. Default: ./<dataset ID>_experiment.yaml."
    )

    args = parser.parse_args()

    if not args.outfile :
        args.outfile = "experiment.yaml"

    logger.info( "Reading pipeline config file " + args.pipelineConfigFilename + "..." )

    with open( args.pipelineConfigFilename, 'r' ) as pipelineConfigFile :
        pipelineConfigInfo = json.load(pipelineConfigFile)

    logger.info( "Finished reading pipeline config file." )

    cytokitConfig = {
            "name" : pipelineConfigInfo[ "name" ],
            "date" : pipelineConfigInfo[ "date" ],
            "environment" : {
                "path_formats" : path_format
            },
            "acquisition" : { }, # This is populated below.
            "processor" : {
                "args" : {
                    "gpus" : gpus,
                    "memory_limit" : memory_limit,
                    "run_crop" : True,
                    "run_tile_generator" : True,
                    "run_drift_comp" : True,
                    "run_cytometry" : True,
                    "run_best_focus" : True,
                    "run_deconvolution" : True
                },
                "deconvolution" : { "n_iter" : 25, "scale_factor" : .5 },
                "tile_generator" : { "raw_file_type" : "keyence_mixed" },
                "best_focus" : { "channel" : pipelineConfigInfo[ "best_focus" ] },
                "drift_compensation" : { "channel" : pipelineConfigInfo[ "drift_compensation" ] },
                "cytometry" : {
                    "target_shape" : pipelineConfigInfo[ "target_shape" ],
                    "nuclei_channel_name" : pipelineConfigInfo[ "nuclei_channel" ],
                    "segmentation_params" : {
                        "memb_min_dist" : 8,
                        "memb_sigma" : 5,
                        "memb_gamma" : .25,
                        "marker_dilation" : 3
                    },
                    "quantification_params" : {
                        "nucleus_intensity" : True,
                        "cell_graph" : True
                    }
                }
            },
            "analysis" : [
                {
                    "aggregate_cytometry_statistics" : {
                        "mode" : "best_z_plane"
                    }
                }
            ]
    }

    if "membrane_channel" in pipelineConfigInfo :
        cytokitConfig[ "processor" ][ "cytometry" ][ "membrane_channel_name" ] = pipelineConfigInfo[ "membrane_channel" ]
    else :
        logger.warning( "No membrane stain channel found in pipeline config. Will only use nuclei channel for segmentation." )

    # Populate acquisition section.
    acquisitionFields = [
                "per_cycle_channel_names",
                "channel_names",
                "axial_resolution",
                "lateral_resolution",
                "emission_wavelengths",
                "magnification",
                "num_cycles",
                "num_z_planes",
                "numerical_aperture",
                "objective_type",
                "region_height",
                "region_names",
                "region_width",
                "tile_height",
                "tile_overlap_x",
                "tile_overlap_y",
                "tile_width",
                "tiling_mode"
                ]

    for field in acquisitionFields :
        cytokitConfig[ "acquisition" ][ field ] = pipelineConfigInfo[ field ]

    # Create operator section to extract channels collapsed in one time point,
    # leaving out blank/empty channels and only including the nuclear stain
    # channel used for segmentation.
    blankPattern = re.compile( r'^blank', re.IGNORECASE )
    emptyPattern = re.compile( r'^empty', re.IGNORECASE )
    dapiChannelPattern = re.compile( r'^DAPI', re.IGNORECASE )
    hoechstChannelPattern = re.compile( r'^HOECHST', re.IGNORECASE )

    operatorExtractChannels = []

    for channelName in pipelineConfigInfo[ "channel_names" ] :

        # Skip unwanted channels.
        if blankPattern.match( channelName ) :
            continue
        elif emptyPattern.match( channelName ) :
            continue
        elif dapiChannelPattern.match( channelName ) :
            if channelName != pipelineConfigInfo[ "nuclei_channel" ] :
                continue
        elif hoechstChannelPattern.match( channelName ) :
            if channelName != pipelineConfigInfo[ "nuclei_channel" ] :
                continue

        # Append to operator extract channels with "proc_" prepended -- this
        # tells Cytokit to extract the channels from the processed tiles.
        operatorExtractChannels.append( "proc_" + channelName )

    # Add operator section to config.
    cytokitConfig[ "operator" ] = [
        {
            "extract" : {
                "name" : "expressions",
                "channels" : operatorExtractChannels
            }
        }
    ]

    logger.info( "Writing Cytokit config to " + args.outfile )

    with open( args.outfile, 'w') as outFile:
        yaml.safe_dump(
            cytokitConfig,
            outFile,
            encoding = "utf-8",
            default_flow_style = None,
            indent = 2
        )

    logger.info( "Finished writing Cytokit config." )
