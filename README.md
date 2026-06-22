# Automatic High-School Timetabling

Mixed-integer programming models for constructing feasible high-school
timetables under complex resource and scheduling constraints.

## Overview

This repository contains the code and dissertation for my University of
Edinburgh MMath project on the High-School Timetabling Problem. The project
investigates whether exact optimisation methods can produce high-quality
timetables for realistic XHSTT benchmark instances.

The central approach is a three-stage decomposition:

1. Assign events to times.
2. Improve the timetable against soft constraints while retaining feasibility.
3. Assign rooms to the scheduled events.

The decomposition reduces the size of the optimisation problem and allows
solutions from earlier stages to warm-start later models.

## Methodology

- Formulated the scheduling problem as mixed-integer programs in Pyomo.
- Used Gurobi to solve the models and evaluate computational performance.
- Enforced hard constraints covering resources, clashes, event durations,
  availability, and room capacity.
- Modelled soft constraints such as undesirable gaps and limits on daily
  workloads.
- Compared decomposed and integrated formulations across multiple XHSTT
  instances.
- Used warm starts and solver configuration to improve tractability.

## Repository Contents

- `Automatic_Timetabling_Dissertation.pdf` - complete dissertation, including
  formulations, experiments, results, and discussion.
- Instance folders - data, model code, and outputs for each benchmark case.
- `README.md` - project overview and repository guidance.

The benchmark folders include Finnish, Danish, UK, and US school instances of
different sizes and structures.

## Running the Models

The implementations are provided as Jupyter notebooks inside the instance
folders. They require Python, Pyomo, and access to a Gurobi installation and
licence.

Open the notebook for the desired instance, update any local data paths, and run
the cells in order.

## Key Skills Demonstrated

Mixed-integer programming, mathematical modelling, decomposition methods,
warm-start design, computational experimentation, Python, Pyomo, and Gurobi.

## Limitations and Further Work

Exact timetabling models remain computationally demanding on large instances.
Further work could include stronger valid inequalities, adaptive decomposition,
symmetry reduction, and hybrid exact-heuristic methods.

## Academic Context

Individual MMath dissertation completed at the University of Edinburgh.
