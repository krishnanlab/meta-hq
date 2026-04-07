# Terms and Conditions

MetaHQ integrates curated sample and study annotations from 13 publicly-available sources.
This document provides complete attribution and licensing information for each source.
Users of MetaHQ must comply with the terms of all constituent licenses.

---

## License Summary

The MetaHQ database is released under **CC BY-NC 4.0** ([Creative Commons Attribution-NonCommercial 4.0 International](database_license.md))
because several source datasets carry NonCommercial restrictions.

**For commercial use:** You must either:

1. Obtain permissions from sources with NonCommercial restrictions
2. Use only annotations from sources with commercial-compatible licenses (CC0, CC BY)

Our software is released under the [BSD 3-Clause](software_license.md).

---

## Usage Guidelines

### For Academic/Non-Commercial Use

You may freely use MetaHQ for:

- Academic research and education
- Non-profit organization research
- Personal study and analysis
- Publishing research findings (with proper attribution)

**Required Attribution:**

When using MetaHQ in publications or presentations, please cite: <br>
Hicks, P. et al. MetaHQ: Harmonized, high-quality metadata annotations of public omics samples and studies. arXiv, (2026).

AND acknowledge: <br>
"This work used annotations from MetaHQ, which integrates curated metadata from [list specific sources used]."

See [Citations](../about/citation.md) for citation information for all sources.

### For Commercial Use

MetaHQ carries NonCommercial restrictions. For commercial applications:

**Option 1 - Obtain Permissions:**
Contact sources with NC restrictions:

- DiSignAtlas (Academic use only)
- Gemma (CC BY-NC 4.0)
- Sirota_2011 (CC BY-NC)
- URSA (CC BY-NC 3.0)
- URSA_HD (CC BY-NC 3.0)

**Option 2 - Use Commercial-Compatible Subset:**
Filter to use only:

- **CC0 sources:** ALE, Bgee, Gu_2023, Golightly_2018
- **CC BY sources:** CellO, CREEDS, Johnson_2023, KrishnanLab

```bash
# Filter for commercial-compatible annotations
metahq retrieve tissues --terms "UBERON:0000948" \
  --source-license="permissive"
```

---

## Quality and Provenance Tracking

MetaHQ tracks annotation source and quality:

**Quality Categories:**

- Expert-curated: Manual annotation by domain experts
- Crowd-sourced: Community-curated annotations

---

## Contact

Please direct all questions to arjun.krishnan@cuanschutz.edu

---

## Disclaimer

Users are responsible for verifying license compliance for their use case.
Licensing terms may change; always check original sources. MetaHQ is provided "as is" without warranty.
