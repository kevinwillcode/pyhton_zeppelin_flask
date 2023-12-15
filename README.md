- docker build -t calculate_jip .

- docker run --name calculate_jip -d -p 5000:5000 calculate_jip

- docker save -o calculate_jip.tar bda/calculate_jip

- docker load -i my-calculate_jip.tar

- docker run -d -p 8080:8080 calculate_jip | docker run --name calculation_jip -d -p 5000:5000 -t kevinity310/calculation_jip
