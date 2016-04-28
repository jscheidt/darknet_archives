lines = Array.new

output = File.open('uniqueChains5000r1', "w+")
input = File.open('run1-5000', "r")
#output = File.open('uniqueChains', "w+")
#input = File.open('top', "r")

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
        tabSplit = lines[index].split("\t")
        cosineSplit = tabSplit[1].split("<====>")
        userSplit = cosineSplit[1].split("<=0=>")
        user1 = userSplit[0].gsub(/"/, '').strip
        user2 = userSplit[1].gsub(/"/, '').strip
        market1 = user1.split("<==>")[0]
        market2 = user2.split("<==>")[0]
        cosine = cosineSplit[0].gsub(/"/, '').strip
        match = cosineSplit[1].gsub(/"/, '').strip

        if groupCosines.empty?
            unless allElements.include?(user1) or allElements.include?(user2)
                groupCosines[match] = cosine
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
            groupCosines[match] = cosine
            lines.delete_at(index)
            index = -1
        elsif groupMarkets.include?(market2) and groupElements.include?(user2) and not groupMarkets.include?(market1) and not groupElements.include?(user1)
            groupMarkets.push(market1)
            allElements.push(user1)
            groupElements.push(user1)
            groupCosines[match] = cosine
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