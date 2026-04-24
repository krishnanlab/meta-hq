# Instructions to add a new source

As of MetaHQ v1 maintains a GEO-forward annotation schema, all IDs must be mapped to GEO IDs. References to SRA are automatically added in the combining phase.

## Study-level specific

For study-forward annotations (where an annotation is provided at the study-level), you must add the path to the list of study sources in `src/metahq_setup/config/config.py` defined as the constant `PROCESSED_STUDY_ANNOTATIONS`.
