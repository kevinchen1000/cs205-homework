#serial code

f = open('A Words.txt')

#print line
for line in f:
  print line
  print ''.join(sorted(line))
  #sort words

