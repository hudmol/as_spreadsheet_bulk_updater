ASpaceGems.setup if defined? ASpaceGems

# If you change these, you may want to rm -rf ./local_gems/ and rerun
# bootstrap.sh to pull down your updated versions.
local_gems = [
  {
    gem: "xlsx_streaming_reader",
    url: "https://github.com/hudmol/xlsx_streaming_reader.git",
    ref: "tags/v1.0",
  },
]


local_gems_path = File.join(File.dirname(__FILE__), "local_gems")

FileUtils.mkdir_p(local_gems_path)

local_gems.each do |gem|
  checkout_dir = File.join(local_gems_path, gem.fetch(:gem))

  if $0 == "gems/bin/bundle"
    # We're running a bootstrap.  Make sure we have the latest version of our
    # dependency.  If it's a symlink, we assume you know what you're doing.
    if File.exist?(File.join(checkout_dir, '.git')) && !File.symlink?(checkout_dir)
      $stderr.puts("Resetting '%s' to %s" % [checkout_dir, gem[:ref]])
      system("git", "-C", checkout_dir, "fetch", "origin")
      system("git", "-C", checkout_dir, "reset", "--hard", gem.fetch(:ref))
    end
  end

  # If the gem hasn't been checked out to local_gems/, grab a copy now.
  unless Dir.exist?(checkout_dir)
    system("git", "clone", gem.fetch(:url), checkout_dir)
    system("git", "-C", checkout_dir, "reset", "--hard", gem.fetch(:ref))
  end

  # Add to load path
  $LOAD_PATH << File.join(checkout_dir, 'lib')
end

# Require dependencies
local_gems.each do |gem|
  require gem.fetch(:gem)
end
