#!/usr/bin/env nextflow

// Using DSL-2
nextflow.enable.dsl=2

include { illumination_first_stitching } from './modules/illumination_first_stitching'

workflow {

    // Illumination correction, best focus selection, and stitching stage 1
    illumination_first_stitching(
        Channel.fromPath(
            params.data_dir,
            checkIfExists: true,
            type: 'dir'
        )
    )

}