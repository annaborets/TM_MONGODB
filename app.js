'use strict'
const util = require('util');
const { mapUser, getRandomFirstName, mapArticle } = require('./util')
const studentsFile = require('./students.json');

// db connection and settings
const connection = require('./config/connection')

let userCollection;
let articlesCollection;
let studentsCollection;
let collectionNames;

async function runUsers() {

  if (collectionNames.includes('users')) {
    await connection.get().dropCollection('users')
  }

  await connection.get().createCollection('users')

  userCollection = connection.get().collection('users')

  await example1();
  await example2();
  await example3();
  await example4()
}

// #### Users

// - Create 2 users per department (a, b, c)
async function example1() {

  try {
    const users = [
      mapUser({ department: 'a' }),
      mapUser({ department: 'a' }),
      mapUser({ department: 'b' }),
      mapUser({ department: 'b' }),
      mapUser({ department: 'c' }),
      mapUser({ department: 'c' }),
    ];

    const options = { ordered: true };

    await userCollection.insertMany(users, options);
  } catch (err) {
    console.error(err)
  }
}

// - Delete 1 user from department (a)

async function example2() {
  try {
    await userCollection.deleteOne({ department: "a" });
  } catch (err) {
    console.error(err)
  }
}

// - Update firstName for users from department (b)

async function example3() {
  try {
    const filter = { department: "b" };
    const updateDoc = {
      $set: {
        firstName: getRandomFirstName(),
      },
    };
    await userCollection.updateMany(filter, updateDoc);
  }
  catch (err) {
    console.error(err)
  }
}

// - Find all users from department (c)
async function example4() {
  try {
    const res = await userCollection.find({ department: 'c' }).toArray();
    console.log('Find all users from department (c)')
    console.log(res)
  } catch (err) {
    console.error(err)
  }
}

//ARTICLES

async function runArticles() {
  if (collectionNames.includes('articles')) {
    await connection.get().dropCollection('articles')
  }

  await connection.get().createCollection('articles')
  articlesCollection = connection.get().collection('articles')

  await example5();
  await example6();
  await example7();
  await example8();
  await example9();
}

//Create 5 articles per each type (a, b, c)
async function example5() {
  try {
    const articles = [
      mapArticle({ type: 'a' }),
      mapArticle({ type: 'a' }),
      mapArticle({ type: 'a' }),
      mapArticle({ type: 'a' }),
      mapArticle({ type: 'a' }),
      mapArticle({ type: 'b' }),
      mapArticle({ type: 'b' }),
      mapArticle({ type: 'b' }),
      mapArticle({ type: 'b' }),
      mapArticle({ type: 'b' }),
      mapArticle({ type: 'c' }),
      mapArticle({ type: 'c' }),
      mapArticle({ type: 'c' }),
      mapArticle({ type: 'c' }),
      mapArticle({ type: 'c' }),
    ];

    const options = { ordered: true };

    await articlesCollection.insertMany(articles, options);
  } catch (err) {
    console.error(err)
  }
}

// Find articles with type a, and update tag list with next value [‘tag1-a’, ‘tag2-a’, ‘tag3’]
async function example6() {
  try {
    const filter = { type: "a" };
    const updateDoc = {
      $set: {
        tags: ['tag1-a', 'tag2-a', 'tag3'],
      },
    };
    await articlesCollection.updateMany(filter, updateDoc);
  }
  catch (err) {
    console.error(err)
  }
}

//Add tags [‘tag2’, ‘tag3’, ‘super’] to other articles except articles from type a
async function example7() {
  try {
    const filter = { type: { $ne: "a" } };
    const updateDoc = {
      $set: {
        tags: ['tag2', 'tag3', 'super'],
      },
    };
    await articlesCollection.updateMany(filter, updateDoc);
  }
  catch (err) {
    console.error(err)
  }
}

// Find all articles that contains tags[tag2, tag1 - a]
async function example8() {
  try {
    const result = await articlesCollection.find({ tags: { $in: ["tag2", "tag1-a"] } }).toArray();
    console.log('Find all articles that contains tags [tag2, tag1 - a]')
    console.log(result)
  } catch (err) {
    console.error(err)
  }
}

// Pull [tag2, tag1-a] from all articles
async function example9() {
  try {
    await articlesCollection.updateMany({}, { $pull: { tags: { $in: ["tag2", "tag1-a"] } } })
  } catch (err) {
    console.error(err)
  }
}


async function runStudents() {
  if (collectionNames.includes('students')) {
    await connection.get().dropCollection('students')
  }

  await connection.get().createCollection('students')
  studentsCollection = connection.get().collection('students')

  const options = { ordered: true };

  await studentsCollection.insertMany(studentsFile, options);

  await example10();
  await example11();
  await example12();
  await example13();
  await example14();
  await example15();
  await example16();
}


// Find all students who have the worst score for homework, sort by descent
async function example10() {
  try {
    const result = await studentsCollection.aggregate(
      [
        {
          '$unwind': {
            'path': '$scores'
          }
        }, {
          '$match': {
            'scores.type': 'homework'
          }
        }, {
          '$sort': {
            'scores.score': 1
          }
        }, {
          '$limit': 5
        }, {
          '$sort': {
            'scores.score': -1
          }
        }
      ]
    ).toArray();
    console.log('Find all students who have the worst score for homework, sort by descent')
    console.log(result)
  } catch (error) {
    console.log(error);
  }
}

// Find all students who have the best score for quiz and the worst for homework, sort by ascending
async function example11() {
  try {
    const result = await studentsCollection.aggregate(
      [
        {
          '$project': {
            'name': 1,
            'scores': {
              '$arrayToObject': {
                '$map': {
                  'input': '$scores',
                  'as': 'el',
                  'in': {
                    'k': '$$el.type',
                    'v': '$$el.score'
                  }
                }
              }
            }
          }
        }, {
          '$sort': {
            'scores.quiz': -1
          }
        }, {
          '$limit': 10
        }, {
          '$sort': {
            'scores.homework': 1
          }
        }, {
          '$limit': 5
        }, {
          '$project': {
            'name': 1,
            'scores': {
              '$map': {
                'input': {
                  '$objectToArray': '$scores'
                },
                'as': 'el',
                'in': {
                  'type': '$$el.k',
                  'score': '$$el.v'
                }
              }
            }
          }
        }
      ]
    ).toArray();
    console.log('Find all students who have the worst score for homework, sort by descent')
    console.log(util.inspect(result, { showHidden: false, depth: null, colors: true }))
  } catch (error) {
    console.log(error);
  }
}

// Find all students who have best scope for quiz and exam
async function example12() {
  try {
    const result = await studentsCollection.aggregate(
      [
        {
          '$project': {
            'name': 1,
            'scores': {
              '$arrayToObject': {
                '$map': {
                  'input': '$scores',
                  'as': 'el',
                  'in': {
                    'k': '$$el.type',
                    'v': '$$el.score'
                  }
                }
              }
            }
          }
        }, {
          '$sort': {
            'scores.quiz': -1
          }
        }, {
          '$limit': 10
        }, {
          '$sort': {
            'scores.exam': -1
          }
        }, {
          '$limit': 5
        }, {
          '$project': {
            'name': 1,
            'scores': {
              '$map': {
                'input': {
                  '$objectToArray': '$scores'
                },
                'as': 'el',
                'in': {
                  'type': '$$el.k',
                  'score': '$$el.v'
                }
              }
            }
          }
        }
      ]
    ).toArray();
    console.log('Find all students who have best scope for quiz and exam')
    console.log(util.inspect(result, { showHidden: false, depth: null, colors: true }))
  } catch (error) {
    console.log(error);
  }
}

// Calculate the average score for homework for all students
async function example13() {
  try {
    const result = await studentsCollection.aggregate(
      [
        {
          '$project': {
            'name': 1,
            'scores': {
              '$arrayToObject': {
                '$map': {
                  'input': '$scores',
                  'as': 'el',
                  'in': {
                    'k': '$$el.type',
                    'v': '$$el.score'
                  }
                }
              }
            }
          }
        }, {
          '$group': {
            '_id': null,
            'avgHomeworkScore': {
              '$avg': '$scores.homework'
            }
          }
        }
      ]
    ).toArray();
    console.log('Calculate the average score for homework for all students')
    console.log(result)
  } catch (error) {
    console.log(error);
  }
}

// Delete all students that have homework score <= 60
async function example14() {
  try {
    const result = await studentsCollection.aggregate(
      [
        {
          '$project': {
            'name': 1,
            'scores': {
              '$arrayToObject': {
                '$map': {
                  'input': '$scores',
                  'as': 'el',
                  'in': {
                    'k': '$$el.type',
                    'v': '$$el.score'
                  }
                }
              }
            }
          }
        }, {
          '$match': {
            'scores.homework': {
              '$lte': 60
            }
          }
        }, {
          '$project': {
            'name': 1,
            'scores': {
              '$map': {
                'input': {
                  '$objectToArray': '$scores'
                },
                'as': 'el',
                'in': {
                  'type': '$$el.k',
                  'score': '$$el.v'
                }
              }
            }
          }
        }
      ]
    ).toArray()

    const ids = result.map(el => el._id)

    await studentsCollection.deleteMany({ _id: { $in: ids } });
  } catch (error) {
    console.log(error);
  }
}

// Mark students that have quiz score => 80
async function example15() {
  try {
    const result = await studentsCollection.aggregate(
      [
        {
          '$project': {
            'name': 1,
            'scores': {
              '$arrayToObject': {
                '$map': {
                  'input': '$scores',
                  'as': 'el',
                  'in': {
                    'k': '$$el.type',
                    'v': '$$el.score'
                  }
                }
              }
            }
          }
        }, {
          '$match': {
            'scores.quiz': {
              '$gte': 80
            }
          }
        }, {
          '$project': {
            'name': 1,
            'scores': {
              '$map': {
                'input': {
                  '$objectToArray': '$scores'
                },
                'as': 'el',
                'in': {
                  'type': '$$el.k',
                  'score': '$$el.v'
                }
              }
            }
          }
        }
      ]
    ).toArray()

    const ids = result.map(el => el._id);

    await studentsCollection.updateMany({ _id: { $in: ids } }, { $set: { marked: true } })
  } catch (error) {
    console.log(error);
  }
}

// - Write a query that group students by 3 categories (calculate the average grade for three subjects)
//   - a => (between 0 and 40)
//   - b => (between 40 and 60)
//   - c => (between 60 and 100)

async function example16() {
  try {
    const result = await studentsCollection.aggregate(
      [
        {
          '$project': {
            'name': 1,
            'scores': '$scores',
            'avgScore': {
              '$avg': '$scores.score'
            }
          }
        }, {
          '$group': {
            '_id': null,
            'a': {
              '$push': {
                '$cond': [
                  {
                    '$lt': [
                      '$avgScore', 40
                    ]
                  }, {
                    '_id': '$_id',
                    'name': '$name',
                    'avgScore': '$avgScore',
                    'scores': '$scores'
                  }, '$$REMOVE'
                ]
              }
            },
            'b': {
              '$push': {
                '$cond': [
                  {
                    '$and': [
                      {
                        '$gte': [
                          '$avgScore', 40
                        ]
                      }, {
                        '$lt': [
                          '$avgScore', 60
                        ]
                      }
                    ]
                  }, {
                    '_id': '$_id',
                    'name': '$name',
                    'avgScore': '$avgScore',
                    'scores': '$scores'
                  }, '$$REMOVE'
                ]
              }
            },
            'c': {
              '$push': {
                '$cond': [
                  {
                    '$gte': [
                      '$avgScore', 60
                    ]
                  }, {
                    '_id': '$_id',
                    'name': '$name',
                    'avgScore': '$avgScore',
                    'scores': '$scores'
                  }, '$$REMOVE'
                ]
              }
            }
          }
        }
      ]
    ).toArray();

    console.log('Students by 3 categories');
    console.log('First 5 students for a category (avg scrore between 0 and 40)');
    console.log(util.inspect(result[0].a.slice(0, 5), { showHidden: false, depth: null, colors: true }))
    console.log('First 5 students for b category (avg scrore between 40 and 60)');
    console.log(util.inspect(result[0].b.slice(0, 5), { showHidden: false, depth: null, colors: true }))
    console.log('First 5 students for c category (avg scrore between 60 and 100)');
    console.log(util.inspect(result[0].c.slice(0, 5), { showHidden: false, depth: null, colors: true }))
  } catch (error) {
    console.log(error);
  }
}


(async () => {
  await connection.connect();

  const collections = await connection.get().listCollections().toArray();
  collectionNames = collections.map(collection => collection.name)

  await runUsers();
  await runArticles();
  await runStudents();
  await connection.close();
})()
