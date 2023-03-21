"""

- works fine, but either highlights trend as 0.5, or removes almost all entries from trend in progress

1. benchmark

*top priority ->
fix the volume weighting, as for now it takes the sum of notional volume for the last
3-month ends, instead of all the volume within these 3 months

*additional ->
add sum of notional volume to the top4.xlsx, fix the data format in plotted chart, add chart and save it
in the top4.xlsx file


3. fix vol_weight_spread for pivots

"""