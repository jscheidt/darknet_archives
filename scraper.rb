require 'action_view'
require 'time'

description = ""
username = ""
date = ""

in_desc = false
output = File.open('panacea_scrape', "a")
base_path = "/s/providence/a/nobackup/cs435/boyarkog/panacea/"
Dir.chdir("#{base_path}")

Dir.foreach('.') do |d|
    date_split = d.split("-")
    date = Time.gm(date_split[0], date_split[1], date_split[2]).to_i
    Dir.chdir("#{base_path}#{d}/")
    Dir.foreach('.') do |file|
        next unless file.include? "p=product"
        input = File.open("#{file}")
        input.readlines.each do |line|
            if line.include? "padding-right: 320px;"
                in_desc = true
            end
            if in_desc
                if line.include? "</div>"
                    in_desc = false
                else
                    description += ActionView::Base.full_sanitizer.sanitize(line.scrub).strip
			#line.gsub(%r{</?[^>]+?>}, '').strip
                end
            end
            if line.include? "Chat with"
                split = line.split(" with ")
                u = split[1].split("<")
                username = u[0]
            end
        end
        if username != "" and description != ""
	    output.puts("\"panacea\",\"#{username}\",\"#{description}\",\"#{date}\",")
	end
        #puts("#{username};#{date};#{description}")
        description = ""
        username = ""
        input.close
    end
    Dir.chdir("#{base_path}")
end

output.close
