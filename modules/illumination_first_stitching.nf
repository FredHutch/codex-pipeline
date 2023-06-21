process collect_dataset_info {
    input:
    path base_directory

    output:
    path "pipelineConfig.json", emit: pipeline_config

    """
python /opt/dataset_info/run_collection.py \
    --path_to_dataset ${base_directory} \
    --num_concurrent_tasks ${params.num_concurrent_tasks}
    """
}

process illumination_correction {
    input:
    path base_directory
    path pipeline_config

    output:
    path "/output/corrected_images", emit: illum_corrected_tiles

    """
python /opt/illumination_correction/run_illumination_correction.py \
    --data_dir ${base_directory} \
    --pipeline_config_path ${pipeline_config}
    """
}

process best_focus {
    input:
    path data_dir
    path pipeline_config

    output:
    path "/output/best_focus", emit: best_focus_tiles

    """
python /opt/best_focus/run_best_focus_selection.py \
    --data_dir ${data_dir} \
    --pipeline_config_path ${pipeline_config}
    """
}

process first_stitching {
    input:
    path data_dir
    path pipeline_config

    output:
    path "/output/stitched_images", emit: stitched_images

    """
python /opt/codex_stitching/run_stitching.py \
    --data_dir ${data_dir} \
    --pipeline_config_path ${pipeline_config}
    """
}

process slicing {
    input:
    path base_stitched_dir
    path pipeline_config

    output:
    path "/output/new_tiles", emit: new_tiles
    path "/output/pipeline_conf/pipelineConfig.json", emit: modified_pipeline_config

    """
python /opt/slicing/run_slicing.py \
    --base_stitched_dir ${data_dir} \
    --pipeline_config_path ${pipeline_config}
    """
}

process create_yaml_config {
    input:
    path pipeline_config

    output:
    path "experiment.yaml", emit: cytokit_config

    """
python /opt/create_cytokit_config.py \
    --gpus ${params.gpus} \
    ${pipeline_config}
    """
}

workflow illumination_first_stitching {
    take:
        data_dir

    main:
        // Collect CODEX dataset info
        collect_dataset_info(data_dir)

        illumination_correction(
            data_dir,
            collect_dataset_info.out.pipeline_config
        )

        best_focus(
            illumination_correction.out.illum_corrected_tiles,
            collect_dataset_info.out.pipeline_config
        )

        first_stitching(
            best_focus.out.best_focus_tiles,
            collect_dataset_info.out.pipeline_config
        )

        slicing(
            first_stitching.out.stitched_images,
            collect_dataset_info.out.pipeline_config
        )

        create_yaml_config(
            slicing.out.modified_pipeline_config
        )

    emit:
        slicing_pipeline_config = slicing.out.modified_pipeline_config
        cytokit_config = create_yaml_config.out.cytokit_config
        new_tiles = slicing.out.new_tiles
}