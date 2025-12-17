# Annotation contributions

The primary goal of the MetaHQ database is to harmonize large collections of curated annotations.
To maintain the quality and scale of the database, we focus on incorporating substantial annotation collections rather than individual or small-scale additions.

**Contribution guidelines:**

- **Datasets:** Collections of >100 datasets
- **Samples:** Collections of >600 samples
- All entries in the submission must be annotated to terms in a **controlled vocabulary**
- All entries must be from **publicly available repositories**

While these are not strict cutoffs, they reflect the scale we aim to maintain. If you have a collection that approaches but doesn't quite meet these thresholds and
you believe it would be valuable to the community, please reach out to discuss — we evaluate contributions on a case-by-case basis.

See our [example annotation submission](#) for guidance on formatting and structure.

### Submitting database contributions

Please submit a pull request to [github.com/krishnanlab/meta-hq](https://github.com/krishnanlab/meta-hq). Use the following template to submit the PR:

```
## Dataset Submission

### Title
[Dataset name]

### Contributors
[List all contributors who should receive authorship credit]

### Description
[Brief description of what this dataset contains and its scientific/research context]

### Dataset Details
- **Number of samples or datasets:**
- **Data type(s):** [RNA-Seq, microarray, histopatholgy images, Hi-C, long-read sequencing, etc.]
- **Evidence code:** [expert-curated, semi-curated, crowd-sourced]
- **Source:** [GEO, SRA, TCGA, PRIDE, MetaboLites, etc.]
- **Relevant publication(s):** [Not required. Include DOI if applicable]

### Standardization
- [ ] All entries are annotated to terms in a controlled vocabulary
- [ ] Annotations are complete and validated according to the evidence code

### Additional Notes
[Any additional context, caveats, or information reviewers should know]

---

**Checklist:**
- [ ] I have read the contributing guidelines
- [ ] All contributors listed have agreed to their inclusion

```

## Contact

If you have any questions, please contact [arjun.krishnan@cuanschutz.edu](mailto:arjun.krishnan@cuanschutz.edu).
