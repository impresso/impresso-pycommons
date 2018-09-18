# JSON Schema of Impresso Canonical Rebuilt Data

Status: to be validated and finalized.

Meaning of keys:

- `id`: ID of the content item (CI)
- `pp`: array of page numbers over which the CI spans; it's the physical
    page number issue-based, as we get it from the OCR.
- `d`: issue date (format: "yyyy-mm-dd")
- `ts`: timestamp of creation of the rebuilt JSON (e.g. "'2018-09-18T08:00:08Z'")
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
  "pp": [
    6
  ],
  "d": "1950-01-23",
  "ts": "2018-09-18T08:32:33Z",
  "lg": "fr",
  "tp": "ar",
  "ppreb": [
    {
      "id": "GDL-1950-01-23-a-p0006",
      "n": 6,
      "t": [
        {
          "c": [
            1489,
            1869,
            1657,
            1899
          ],
          "s": 0,
          "l": 9
        },
        {
          "c": [
            1668,
            1869,
            1714,
            1899
          ],
          "s": 10,
          "l": 3
        },...
      ],
      "r": [
        [
          1422,
          1841,
          2042,
          2041
        ],
        [
          1420,
          2041,
          1722,
          2490
        ],
        [
          1418,
          2506,
          1718,
          2930
        ],
        [
          1420,
          2946,
          1718,
          3218
        ],
        [
          1742,
          2041,
          2044,
          2368
        ],
        [
          1734,
          2378,
          2050,
          2774
        ]
      ]
    }
  ],
  "lb": [
    32,
    59,
    87,
    130,
    153,
    188,
    227,
    263,
    300,
    336,
    370,
    408,
    446,
    483,
    519,
    558,
    595,
    632,
    671,
    708,
    748,
    782,
    822,
    860,
    898,
    935,
    965,
    992,
    1026,
    1061,
    1097,
    1134,
    1174,
    1207,
    1248,
    1285,
    1322,
    1361,
    1398,
    1441,
    1479,
    1518,
    1555,
    1593,
    1630,
    1666,
    1708,
    1751,
    1774,
    1785,
    1809,
    1845,
    1881,
    1919,
    1954,
    1992,
    2029,
    2044,
    2080,
    2117,
    2128,
    2162,
    2200,
    2236,
    2274,
    2281,
    2300,
    2333,
    2368,
    2406,
    2441,
    2476,
    2512,
    2528,
    2563,
    2601,
    2642,
    2672,
    2710,
    2752,
    2773,
    2807,
    2848,
    2882,
    2920,
    2964,
    3004,
    3039,
    3078,
    3115,
    3122,
    3157,
    3193,
    3228,
    3266,
    3303,
    3341,
    3375,
    3413,
    3424
  ],
  "t": "L'interrogatoire de Watson",
  "pb": [
    33,
    60,
    88,
    154,
    337,
    993,
    1810,
    2045,
    2129,
    2282,
    2301,
    2529,
    2774,
    3123
  ],
  "rb": [
    131,
    966,
    1775,
    2194,
    2753
  ],
  "ft": "L'AFFAIRE DES BIJOUX DE LA BEGUM L'interrogatoire de Watson un des principaux complices II se prétend le filleul de M. Churchill / Marseille, 22 janvier. La police, on le sait, a écroué un sixième personnage impliqué dans l'affaire du vol des bijoux de la Bégum : Lindsay Watson, arrêté à Strasbourg, commandant de cavalerie de réserve. Interrogé durant plusieurs heures dans les locaux de la police mobile à Marseille, il aurait reconnu qu'il se trouvait à Cannes le 3 août dernier, jour où fut commise l'agression. Il avait obtenu de la secrétaire de l'Aga Khan les détails sur le départ de ce dernier et de la Bégum, qui devaient prendre l'avion pour Deauville, tandis que cette secrétaire et le chauffeur devaient gagner par la route la station normande. Watson aurait également déclaré qu'il avait, avant l'agression, vu Paul Leca, à plusieurs reprises, à Marseille. Paul Leca, on le sait, est toujours recherché et certains estiment qu'il est le chef du gang. PROMENADE SUR LA CROISETTE On se rappelle également que Watson a été nettement mis en cause par Ruberti lors de son interrogatoire. Sur la Croisette, Paul Leca présenta Watson à Ruberti en ces termes : « Voici l'homme dont je t'ai parlé pour l'affaire. > La veille de l'attaque, Ruberti rencontra encore, sur la célèbre promenade cannoise, Watson qui était en compagnie de la secrétaire de l'Aga Khan. Cette dernière, dont on ignore l'identité, avait été, à Paris, au service des beaux-parents de Watson. Elle aurait été entendue par les policiers qui, dès l'agression commise, avaient ouvert l'enquête et enregistré les déclarations du personnel attaché à la maison du prince. Enfin, Watson n'aurait pas caché aux policiers qu'il se livrait, à Cannes, à différents trafics sans d'ailleurs préciser lesquels. PAS UN MOT AU JUGE D'INSTRUCTION ! Transféré en fin de matinée au Parquet, Watson s'est refusé à toute déclaration au juge d'instruction hors la présence de son défenseur qui sera Me Paul Giaccobi, député de la Corse, ancien ministre qu'il aurait connu à Alger en 1944. Watson a été écroué à la prison des Baumettes sous l'inculpation de plicité de vol. Quant aux bijoux, nulle nouvelle. Les policiers n'en parlent pas davantage que de la suite de l'enquête, au sujet de laquelle ils sont absolument muets. Paris, 22 janvier. (A. FP.) — Les inspecteurs de la sûreté nationale qui enquêtent sur l'affaire des bijoux de la Bégum, ont arrêté samedi soir en Gare de Lyon deux individus qui descendaient du train de Marseille : Antoine Cardoliani et Carbone. D'autre part, dimanche matin, deux occupants d'une voiture qui avait été signalée par la police marseillaise comme appartenant à des membres du gang, ont été appréhendés à Paris. On ignore encore l'identité de ces derniers. UNE BELLE CARRIÈRE ! Georges Lindsay Watson, soupçonné d'avoir été l'instigateur du vol des bijoux de la Bégum, est né le 14 mars 1899 à Paris. Commandant de cavalerie de réserve, il serait officier de la Légion d'honneur et titulaire de la « Military Cross ». En outre, Watson a été en 1936 et 1938 chef du contentieux d'une compagnie d'assurances britannique à Paris. Watson, qui est d'ascendance écossaise, se présentait comme filleul de M. Churchill. H aurait été attaché comme capitaine à l'état-major du général Giraud, à Alger, puis aurait été chargé des relations avec les correspondants de presse étrangers durant les campagnes de Tunisie, d'Italie et de France. "
}
```
