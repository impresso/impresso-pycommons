# JSON Schema of Impresso Canonical Rebuilt Data

Status: to be validated and finalized.

Meaning of keys:

- `id`: ID of the content item (CI)
- `pp`: array of page numbers over which the CI spans
- `d`: date timestamp
- `lg`: two letter language code
- `tp`: content item type (e.g. "ar" for article, "ad" for advertisement)
- `ft`: the rebuilt fulltext
- `lb`: text offsets of physical line breaks (relative to `ft` field)
- `pb`: text offsets of physical paragraph breaks (relative to `ft` field)
- `rb`: text offsets of page regions (relative to `ft` field)
- `ppreb`: a list of rebuilt pages, where each page has:
    - `id`: canonical ID
    - `n`: page number (int)
    - `t`: a list of tokens, where each token has:
        - `c`: page coordinates of token
        - `s`: offset start (relative to `ft` field)
        - `l`: token length
    - `r`: a list of int arrays, with coordinates of page regions over which
        the article spans (as identified by the OCR engine). 

```json
{
    "id": "GDL-1950-01-23-a-i0076",
    "pp": [6],
    "d": "1950-01-23T05:00:00Z",
    "lg": "fr",
    "tp": "ar",
    "ppreb": [{
        "id": "GDL-1950-01-23-a-p0006",
        "n": 6,
        "t": [{
            "c": [1489, 1869, 1657, 1899],
            "s": 0,
            "l": 9
        }, {
            "c": [1668, 1869, 1714, 1899],
            "s": 10,
            "l": 3
        }, ...],
        "r": [
            [1422, 1841, 2042, 2041],
            [1420, 2041, 1722, 2490],
            [1418, 2506, 1718, 2930],
            [1420, 2946, 1718, 3218],
            [1742, 2041, 2044, 2368],
            [1734, 2378, 2050, 2774]
        ]
    }],
    "lb": [13, 52, 66, 93, 141, 163, 207, 234, 278, 314, 353, 379, 419, 457, 491, 532, 567, 606, 648, 681, 724, 758, 796, 830, 871, 917, 944, 979, 998, 1030, 1074, 1104, 1143, 1183, 1218, 1262, 1304, 1335, 1367, 1410, 1459, 1486, 1534, 1568, 1602, 1635, 1676, 1717, 1764, 1781, 1793, 1822, 1849, 1892, 1931, 1962, 2008, 2038, 2053, 2095, 2123, 2138, 2176, 2207, 2245, 2281, 2288, 2303, 2350, 2382, 2420, 2456, 2485, 2519, 2541, 2579, 2614, 2655, 2678, 2724, 2762, 2789, 2819, 2854, 2889, 2931, 2977, 3012, 3047, 3101, 3122, 3130, 3162, 3196, 3244, 3277, 3314, 3351, 3389, 3423],
    "t": "L'interrogatoire de Watson",
    "pb": [33, 60, 88, 154, 337, 993, 1810, 2045, 2129, 2282, 2301, 2529, 2774, 3123],
    "rb": [131, 966, 1775, 2194, 2753],
    "ft": "L'AFFAIRE DES BIJOUX DE LA BEGUM L'interrogatoire de Watson un des principaux complices II se prétend le filleul de M. Churchill / Marseille, 22 janvier. La police, on le sait, a écroué un sixième personnage impliqué dans l'affaire du vol des bijoux de la Bégum : Lindsay Watson, arrêté à Strasbourg, commandant de cavalerie de réserve. Interrogé durant plusieurs heures dans les locaux de la police mobile à Marseille, il aurait reconnu qu'il se trouvait à Cannes le 3 août dernier, jour où fut commise l'agression. Il avait obtenu de la secrétaire de l'Aga Khan les détails sur le départ de ce dernier et de la Bégum, qui devaient prendre l'avion pour Deauville, tandis que cette secrétaire et le chauffeur devaient gagner par la route la station normande. Watson aurait également déclaré qu'il avait, avant l'agression, vu Paul Leca, à plusieurs reprises, à Marseille. Paul Leca, on le sait, est toujours recherché et certains estiment qu'il est le chef du gang. PROMENADE SUR LA CROISETTE On se rappelle également que Watson a été nettement mis en cause par Ruberti lors de son interrogatoire. Sur la Croisette, Paul Leca présenta Watson à Ruberti en ces termes : « Voici l'homme dont je t'ai parlé pour l'affaire. > La veille de l'attaque, Ruberti rencontra encore, sur la célèbre promenade cannoise, Watson qui était en compagnie de la secrétaire de l'Aga Khan. Cette dernière, dont on ignore l'identité, avait été, à Paris, au service des beaux-parents de Watson. Elle aurait été entendue par les policiers qui, dès l'agression commise, avaient ouvert l'enquête et enregistré les déclarations du personnel attaché à la maison du prince. Enfin, Watson n'aurait pas caché aux policiers qu'il se livrait, à Cannes, à différents trafics sans d'ailleurs préciser lesquels. PAS UN MOT AU JUGE D'INSTRUCTION ! Transféré en fin de matinée au Parquet, Watson s'est refusé à toute déclaration au juge d'instruction hors la présence de son défenseur qui sera Me Paul Giaccobi, député de la Corse, ancien ministre qu'il aurait connu à Alger en 1944. Watson a été écroué à la prison des Baumettes sous l'inculpation de plicité de vol. Quant aux bijoux, nulle nouvelle. Les policiers n'en parlent pas davantage que de la suite de l'enquête, au sujet de laquelle ils sont absolument muets. Paris, 22 janvier. (A. FP.) — Les inspecteurs de la sûreté nationale qui enquêtent sur l'affaire des bijoux de la Bégum, ont arrêté samedi soir en Gare de Lyon deux individus qui descendaient du train de Marseille : Antoine Cardoliani et Carbone. D'autre part, dimanche matin, deux occupants d'une voiture qui avait été signalée par la police marseillaise comme appartenant à des membres du gang, ont été appréhendés à Paris. On ignore encore l'identité de ces derniers. UNE BELLE CARRIÈRE ! Georges Lindsay Watson, soupçonné d'avoir été l'instigateur du vol des bijoux de la Bégum, est né le 14 mars 1899 à Paris. Commandant de cavalerie de réserve, il serait officier de la Légion d'honneur et titulaire de la « Military Cross ». En outre, Watson a été en 1936 et 1938 chef du contentieux d'une compagnie d'assurances britannique à Paris. Watson, qui est d'ascendance écossaise, se présentait comme filleul de M. Churchill. H aurait été attaché comme capitaine à l'état-major du général Giraud, à Alger, puis aurait été chargé des relations avec les correspondants de presse étrangers durant les campagnes de Tunisie, d'Italie et de France. "
}
```
