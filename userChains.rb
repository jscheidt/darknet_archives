chains = Hash.new
heads = Array.new
not_heads = Array.new

output = File.open('chains', "w+")
input = File.open('sampleData', "r")

input.readlines.each do |line|
	cosineSplit = line.split("<====>")
	userSplit = cosineSplit[0].split("<=0=>")
	user1 = userSplit[0].gsub(/"/, '').strip
	user2 = userSplit[1].gsub(/"/, '').strip
	#puts(user1)
	#puts(user2)

	unless not_heads.include?(user1)
		heads.push(user1)
	end
	not_heads.push(user2)

	unless chains.key?(user1)
		chains[user1] = user2
	end
end

c = Array.new

for h in heads
	link = h
	c.push(link)
	while chains.key?(link)
		link = chains[link]
		c.push(link)
	end
	output.puts(c.inspect)
	c.clear
end

#output.puts(chains)
#puts(heads)
#puts(not_heads)

input.close
output.close
