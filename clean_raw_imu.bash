
echo "Remove all-zero rows"
grep -v "0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000" $1 > $2

echo "Split files into 1G small files"
split -C 1G --numeric-suffixes --additional-suffix=.csv $1 $2

echo "Combine small files back into single large file"
cat $1 > $2