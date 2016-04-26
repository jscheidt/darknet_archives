require 'find'
homeDir = Dir.pwd
Dir.chdir("/media/greg/New Volume/panacea/")
Dir.foreach(".") do |file|
    puts file
end