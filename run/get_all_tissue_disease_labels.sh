#!/bin/bash

# This script will retrieve all tissue and disease LABELS in MetaHQ

levels=("sample" "series")
attributes=("tissues" "diseases")
technologies=("rnaseq", "microarray")
organisms=("human" "mouse" "rat" "zebrafish" "worm" "fly")

outdir="results/mass_query"

for level in "${levels[@]}"; do
    echo "Level: ${level}"
    for attribute in "${attributes[@]}"; do
        echo "Attribute: ${attribute}"
        for tech in "${technologies[@]}"; do
            echo "Technology: ${tech}"
            for species in "${organisms[@]}"; do
                echo "Species: ${species}"
                metahq retrieve "$attribute" \
                    --terms "all" --output "${outdir}/level-${level}__attribute-${attribute}__tech-${tech}__species-${species}" \
                    --filters "species=${species},tech=${tech},ecode=any" --level "$level" --mode label \
            done
        done
    done
done

python scripts/combine_tissue_disease_annotations.py
