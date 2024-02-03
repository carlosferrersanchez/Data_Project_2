import apache_beam as beam
from pykml import parser
from geopy.distance import geodesic
from lxml import etree

class FindFirstClosestPointFn(beam.DoFn):
    def __init__(self, route2, max_distance=50):
        self.route2 = route2
        self.max_distance = max_distance

    def process(self, point1):
        for point2 in self.route2:
            distance = geodesic(point1, point2).meters
            if distance <= self.max_distance:
                yield point1, point2, distance
                return  


def parse_kml_coordinates(file_path):
    with open(file_path) as f:
        doc = parser.parse(f).getroot()
        coordinates = []
        ns = {'kml': 'http://www.opengis.net/kml/2.2'}
        for coord in doc.xpath('//kml:coordinates', namespaces=ns):
            for coord_set in coord.text.strip().split():
                lon, lat, _ = coord_set.split(',')
                coordinates.append((float(lat), float(lon)))
        return coordinates

route1 = parse_kml_coordinates('conductor.kml')
route2 = parse_kml_coordinates('usuario.kml')

with beam.Pipeline() as pipeline:
    matches = (
        pipeline
        | 'Crear PCollection para ruta1' >> beam.Create(route1)
        | 'Encontrar primera coincidencia cercana' >> beam.ParDo(FindFirstClosestPointFn(route2))
    )

    first_match = (
        matches
        | 'Filtrar para obtener el primer resultado' >> beam.combiners.Top.Of(1, key=lambda x: x[2])
        | 'Aplanar los resultados' >> beam.FlatMap(lambda x: x)
    )

    first_match | 'Imprimir la primera coincidencia' >> beam.Map(print)

