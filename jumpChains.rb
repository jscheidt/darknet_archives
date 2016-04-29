=begin
Script to filter top matches. Users that have activity under 10000 are discarded....
Script not finished atm
=end

#####################################################

marketDates = Hash.new
input = File.open('userDate/userDateGlobal', "r")

input.readlines.each do |line|
	tabSplit = line.strip.split("\t")
	marketDates[tabSplit[0]] = tabSplit[1]
end

input.close

#####################################################
lines = Array.new

output = File.open('finalSortedDataJumps.out', 'w+')
input = File.open('finalSortedData1','r')
#output = File.open('jumps', "w+")
#input = File.open('simactivity/sortedMerged', "r")
#output = File.open('jumps5000r2', "w+")
#input = File.open('run2-5000', "r")

input.readlines.each do |line|
    lines.push(line)
end

allElements = Array.new

groupCosines = Hash.new
groupMarkets = Array.new
groupElements = Array.new
index = 0

for l in lines
    while index < lines.size && lines.size > 0
    	split = l.gsub(/"/, '').to_s.strip.split(/\t|<=0=>|<====>|<===>|<==>|<=A=>|<F=L>/)
    	cosine = split[1]
    	market1 = split[2]
    	user1 = l.gsub(/"/, '').to_s.strip.split(/\t|<=0=>|<====>/)[2]
    	activity1 = split[4]
    	firstDiff1 = split[5]
    	firstPost1 = marketDates[market1].to_s.split(":")[0].to_i + firstDiff1.to_i
    	lastDiff1 = split[6]
    	lastPost1 = marketDates[market1].to_s.split(":")[1].to_i - lastDiff1.to_i
    	market2 = split[7]
    	user2 = l.gsub(/"/, '').strip.split(/\t|<=0=>|<====>/)[3]
    	activity2 = split[9]
    	firstDiff2 = split[10]
    	firstPost2 = marketDates[market2].to_s.split(":")[0].to_i + firstDiff2.to_i
    	lastDiff2 = split[11]
    	lastPost2 = marketDates[market2].to_s.split(":")[1].to_i - lastDiff2.to_i

    	#if lastDiff2.to_i > 17000000# and lastDiff1.to_i > 17000000 or activity1.to_i < 4250000 or activity2.to_i < 4250000
    		#this is to filter out users who did not post their last post on one market and their first on another within ~4 weeks OR
    		#have had less than ~1 week of activity on market
    		#lines.delete_at(index)
        if groupCosines.empty?
            unless allElements.include?(user1) or allElements.include?(user2)
                groupCosines["#{user1}<=0=>#{user2}"] = cosine
                allElements.push(user1)
                allElements.push(user2)
                groupElements.push(user1)
                groupElements.push(user2)
                groupMarkets.push(market1)
                groupMarkets.push(market2)
                lines.delete_at(index)
            end
        elsif groupMarkets.include?(market1) and groupElements.include?(user1) and not groupMarkets.include?(market2) and not groupElements.include?(user2)
            groupMarkets.push(market2)
            allElements.push(user2)
            groupElements.push(user2)
            groupCosines["#{user1}<=0=>#{user2}"] = cosine
            lines.delete_at(index)
            index = -1
        elsif groupMarkets.include?(market2) and groupElements.include?(user2) and not groupMarkets.include?(market1) and not groupElements.include?(user1)
            groupMarkets.push(market1)
            allElements.push(user1)
            groupElements.push(user1)
            groupCosines["#{user1}<=0=>#{user2}"] = cosine
            lines.delete_at(index)
            index = -1
        end
        index += 1
    end

    unless groupCosines.empty?
        output.puts(groupCosines)
    end
    groupCosines.clear
    groupMarkets.clear
    groupElements.clear
    index = 0
end

input.close
output.close
