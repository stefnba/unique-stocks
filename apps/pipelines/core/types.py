from typing import Literal

type Environment = Literal["dev", "prod", "docker_dev"]
type LakeBackend = Literal["local", "motherduck"]
type DbtTarget = Literal["dev", "prod"]
type PipelineLogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR"]
type PipelineLogFormat = Literal["auto", "console", "json"]
