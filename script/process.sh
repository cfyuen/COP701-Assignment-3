for x in {a..z}
do
  cmd="paste --delimiter=\\n --serial data/$x*.json > combine/$x.txt"
  echo At $x now
  eval $cmd
done
