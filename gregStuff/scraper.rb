description = ""
username = ""
date = ""

in_desc = false
output = File.open('panacea_scrape', "a")
base_path = "/media/greg/New Volume/panacea/"
Dir.chdir("#{base_path}")

Dir.foreach('.') do |d|
    date = d
    Dir.chdir("#{base_path}#{d}/")
    Dir.foreach('.') do |file|
        #puts("hi")
        next unless file.include? "p=product"
        #puts("hi2")
        input = File.open("#{file}")
        input.readlines.each do |line|
            #puts("hi3")
            if line.include? "padding-right: 320px;"
                in_desc = true
            end
            if in_desc
                if line.include? "</div>"
                    in_desc = false
                else
                    description += line.gsub(%r{</?[^>]+?>}, '').strip
                end
            end
            if line.include? "Chat with"
                split = line.split(" with ")
                u = split[1].split("<")
                username = u[0]
            end
        end
        output.puts("#{username};#{date};#{description}")
        #puts("#{username};#{date};#{description}")
        description = ""
        username = ""
        date = ""
        input.close
    end
    Dir.chdir("#{base_path}")
end

output.close