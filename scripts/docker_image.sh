name="impresso_pycommons"
image_version="v1"
image_name=$name:$image_version

echo $image_name

sudo docker build . -t $image_name

sudo docker run $image_name pip freeze

sudo docker tag $image_name ic-registry.epfl.ch/dhlab/$image_name

sudo docker push ic-registry.epfl.ch/dhlab/$image_name
