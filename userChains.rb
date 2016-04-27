chains = Hash.new
heads = Array.new
not_heads = Array.new

output = File.open('chains', "w+")
input = File.open('top', "r")

input.readlines.each do |line|
	tabSplit = line.split("\t")
	cosineSplit = tabSplit[1].split("<====>")
	userSplit = cosineSplit[1].split("<=0=>")
	user1 = userSplit[0].gsub(/"/, '').strip
	user2 = userSplit[1].gsub(/"/, '').strip

	unless not_heads.include?(user1) || heads.include?(user1)
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

input.close
output.close
