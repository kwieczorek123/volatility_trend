"""

2. amend the chop to account for trend in progress

- create a logic for counting failed trend in process entries and impact on pivots (how many were there, what was their
impact on pivots), calculate the separate metric for them in pivots
- create different pivots with adjustments done to CI formula, and compare whether it is beneficial to have them

1. benchmark

*top priority ->
fix the volume weighting, as for now it takes the sum of notional volume for the last
3-month ends, instead of all the volume within these 3 months

*additional ->
add sum of notional volume to the top4.xlsx, fix the data format in plotted chart, add chart and save it
in the top4.xlsx file


3. fix vol_weight_spread for pivots

"""