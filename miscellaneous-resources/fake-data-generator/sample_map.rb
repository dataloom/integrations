require_relative 'fake-loom'

@coordinates = []

10000.times do
  long = Faker::Address.longitude.to_f
  lat = Faker::Address.latitude.to_f
  # coordinates = Geocoder.coordinates(Faker::Internet.ip_v4_address)
  # long = coordinates[1]
  # lat = coordinates[0]
  if long.between?(-118, -100) && lat.between?(30, 45)
    @coordinates.push [long, lat]
  end
end

CSV.open("coordinates.csv", "w") do |csv|
  csv << ["Case #", "Car Manufacturer", "Date", "Longitude", "Latitude"]
  @coordinates.each do |coord|
    csv << [Faker::Number.unique.number(10), Faker::Vehicle.manufacture, Faker::Date.backward(30), coord[0], coord[1]]
  end
end
