require_relative 'fake-loom'

# Create 20 interesting people common across both datasets
people = create_people(20)

############ HEALTH DATA ################

# Store health and jail data
@health_data = []

# For a person array
# ID, First, Last, DOB, Address, Check In Date
def add_health_data(person)
  @health_data.push [
                      Faker::Number.unique.number(10),
                      person[0],
                      person[1],
                      person[3],
                      create_full_address,
                      create_random_date(360).strftime("%Y-%d-%m")
                    ]
end

# Health Data (Tracked)
people.each do |person|
  add_health_data(person)
end

# Add a few repeat patients
10.times do
  person = people.sample
  add_health_data(person)
end

# Health Data (Noise)
100.times do
  @health_data.push [
                Faker::Number.unique.number(10),
                Faker::Name.first_name,
                Faker::Name.last_name,
                Faker::Date.birthday,
                create_full_address,
                create_random_date(360).strftime("%Y-%d-%m")
             ]
end

# shuffle health data
@health_data = @health_data.shuffle

# add health data to csv
CSV.open("fake_health_data.csv", "w") do |csv|
  csv << ["Patient ID", "First Name", "Last Name", "DOB", "Address", "Check In Date"]
  @health_data.each do |data|
    csv << [data[0], data[1], data[2], data[3], data[4], data[5]]
  end
end

########## JAIL DATA ############

@jail_data = []

# For a person array
# ID, First, Last, Race, DOB, Address, Date Booked, Date Released
def add_jail_data(person)
  datebooked = create_random_date(360)
  datereleased = datebooked + rand(0...30)
  @jail_data.push [
                      Faker::Number.unique.number(10),
                      person[0],
                      person[1],
                      person[2],
                      person[3],
                      create_full_address,
                      datebooked.strftime("%Y-%d-%m"),
                      datereleased.strftime("%Y-%d-%m")
                  ]
end

# Jail data (Tracked)
people.each do |person|
  datebooked = create_random_date(360)
  datereleased = datebooked + rand(0...30)
  add_jail_data(person)
end

# Add a few repeat offenders
10.times do
  person = people.sample
  add_jail_data(person)
end

# Jail data (Noise)
100.times do
  datebooked = create_random_date(360)
  datereleased = datebooked + rand(0...30)
  @jail_data.push [
                Faker::Number.unique.number(10),
                Faker::Name.first_name,
                Faker::Name.last_name,
                Faker::Demographic.race,
                Faker::Date.birthday,
                create_full_address,
                datebooked.strftime("%Y-%d-%m"),
                datereleased.strftime("%Y-%d-%m")
             ]
end

# Shuffle jail data
@jail_data = @jail_data.shuffle

# Add jail data to csv
CSV.open("fake_jail_data.csv", "w") do |csv|
  csv << ["Booking ID", "First Name", "Last Name", "Race", "DOB", "Address", "Date Booked", "Date Released"]
  @jail_data.each do |data|
    csv << [data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]]
  end
end
