cwlVersion: v1.1
class: CommandLineTool
label: Collect dataset info for Cytokit

requirements:
  DockerRequirement:
    dockerPull: hubmap/codex-scripts

baseCommand: ["python", "/opt/dataset_info/run_collection.py"]

inputs:
  base_directory:
    type: Directory
    inputBinding:
      prefix: "--path_to_dataset"

outputs:
  pipeline_config:
    type: File
    outputBinding:
      glob: pipelineConfig.json
