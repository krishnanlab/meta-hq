# Contributing

Thank you for contributing to MetaHQ!

## Ways to contribute

There are two main ways to contribute to MetaHQ.

1. Add new standardized sample and/or dataset annotations to the database.
2. Add to the codebase.

Substantial contributions through these means will result in authorship on future MetaHQ releases and publications.

## Database contributions

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

## Codebase contributions

We welcome code contributions that enhance MetaHQ's functionality, fix bugs, or improve performance. Examples of substantial codebase contributions include:

- New features or modules
- Bug fixes
- Performance optimizations
- Test coverage improvements
- Significant refactoring

**Note on documentation changes:** While we appreciate improvements to documentation, changes limited to README files, typo fixes, or minor formatting adjustments alone do not typically qualify for authorship.
However, documentation contributions paired with code changes, or comprehensive documentation efforts (e.g., adding detailed API documentation, tutorials, or usage guides) are valued and considered substantial contributions.

### Submitting code contributions

To submit codebase contributions, please use the following PR template:

```
# What
[High-level explanation of the PR]

# Why
[Explain why these changes are necessary]

# How
[High-level explanation of the approach taken]

## Changes made
[Detailed description of the changes made]

# PR Checklist
- [ ] Explained the purpose of this PR
- [ ] Self-reviewed this PR
- [ ] Added/updated tests
- [ ] Updated documentation (if applicable)
```
