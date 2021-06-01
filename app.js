'use strict';

const {mapUser, getRandomFirstName, mapArticle} = require('./util');

// db connection and settings
const connection = require('./config/connection');
let userCollection;
let articleCollection;
let studentsCollection;
run();

async function run() {
  await connection.connect();
  await connection.get().dropCollection('users');
  await connection.get().createCollection('users');
  await connection.get().dropCollection('articles');
  await connection.get().createCollection('articles');
  userCollection = connection.get().collection('users');
  articleCollection = connection.get().collection('articles');
  studentsCollection = connection.get().collection('students');

  await example1();
  await example2();
  await example3();
  await example4();
  await example5();
  await example6();
  await example7();
  await example8();
  await example9();
  await example10();
  await example11();
  await example12();
  await example13();
  await example14();
  await example15();
  await example16();
  await connection.close();
}

// #### Users

// - Create 2 users per department (a, b, c)
async function example1() {
  try {
    const departments = ['a', 'a', 'b', 'b', 'c', 'c'];
    const users = departments.map(d => ({department: d})).map(mapUser);
    try {
      const {result} = await userCollection.insertMany(users);
      console.log(`Added ${result.n} users`);
    } catch (err) {
      console.error(err);
    }
  } catch (err) {
    console.error(err);
  }
}

// - Delete 1 user from department (a)

async function example2() {
  try {
    const {result} = await userCollection.deleteOne({department: 'a'});
    console.log(`Removed ${result.n} users`);
  } catch (err) {
    console.error(err);
  }
}

// - Update firstName for users from department (b)

async function example3() {
  try {
    const [query, update] = [{department: 'b'}, {$set: {firstName: getRandomFirstName()}}];
    const {result} = await userCollection.updateMany(query, update);
    console.log(`Updated ${result.n} users`);
  } catch (err) {
    console.error(err);
  }
}

// - Find all users from department (c)
async function example4() {
  try {
    const result = await userCollection.find({department: 'c'});
    await result.forEach(doc => console.log(doc));
  } catch (err) {
    console.error(err);
  }
}

async function example5() {
  try {
    const types = ['a', 'b', 'c'].map(type => Array(5).fill(type)).flat();
    const articles = types.map(d => ({type: d})).map(mapArticle);
    try {
      const {result} = await articleCollection.insertMany(articles);
      console.log(`Added ${result.n} articles`);
    } catch (err) {
      console.error(err);
    }
  } catch (err) {
    console.error(err);
  }
}

async function example6() {
  try {
    const {result} = await articleCollection.updateMany(
      {type: 'a'},
      {
        $set: {
          tags: ['tag1-a', 'tag2-a', 'tag3']
        }
      }
    );
    console.log(`Updated ${result.n} articles with type a`);
  } catch (error) {
    console.log(error);
  }
}

async function example7() {
  try {
    const {result} = await articleCollection.updateMany(
      {type: {$ne: 'a'}},
      {
        $set: {
          tags: ['tag2', 'tag3', 'super']
        }
      }
    );
    console.log(`Updated ${result.n} articles with type !a`);
  } catch (error) {
    console.log(error);
  }
}

//picks documents if any of two tag values included in array, if I got it right
async function example8() {
  try {
    const result = await articleCollection.find({
      tags: {$in: ['tag2', 'tag1-a']}
    });
    const count = await result.count();
    console.log(`Updated ${count} articles with type ['tag2', 'tag1-a']`);
  } catch (error) {
    console.log(error);
  }
}

async function example9() {
  try {
    const {result} = await articleCollection.updateMany(
      {},
      {$pull: {tags: {$in: ['tag2', 'tag1-a']}}},
      {multi: true}
    );
    console.log(`Pulled ['tag2', 'tag1-a'] from ${result.n} articles`);
  } catch (error) {
    console.log(error);
  }
}

// - Import all data from students.json into student collection

// I imported data manually via Compass, tried to do it via insertmany programatically
//but it didn't work

//- Import all data from students.json into student collection
// async function example10() {
//   try {
//     const result = await studentsCollection.insertMany(students);
//     console.log(`InsertedCount-${result.insertedCount}`);
//   } catch (error) {
//     console.log(error);
//   }
// }

// Find all students who have the worst score for homework, sort by descent
async function example10() {
  try {
    const result = await studentsCollection.aggregate([
      {$unwind: '$scores'},
      {
        $match: {
          'scores.type': 'homework'
        }
      },
      {
        $sort: {
          'scores.score': -1
        }
      }
    ]);
    // await result.forEach(doc => console.log(doc));
  } catch (error) {
    console.log(error);
  }
}

// Find all students who have the best score for quiz and the worst for homework, sort by ascending

async function example11() {
  try {
    const result = await studentsCollection.aggregate([
      {$unwind: '$scores'},
      {$match: {$or: [{'scores.type': 'quiz'}, {'scores.type': 'homework'}]}},
      {
        $group: {
          _id: {_id: '$name'},
          name: {$first: '$name'},
          quiz: {$first: '$scores.score'},
          hw: {$last: '$scores.score'}
        }
      },
      {
        $sort: {
          quiz: 1,
          hw: -1
        }
      }
    ]);
    // await result.forEach(doc => console.log(doc));
  } catch (error) {
    console.log(error);
  }
}

// Find all students who have best score for quiz and exam

// for this function I assume that both quiz and exam should be summed and sorted
async function example12() {
  try {
    const result = await studentsCollection.aggregate([
      {$unwind: '$scores'},
      {$match: {$or: [{'scores.type': 'quiz'}, {'scores.type': 'exam'}]}},
      {
        $group: {
          _id: {_id: '$_id'},
          name: {$first: '$name'},
          sum: {$sum: '$scores.score'}
        }
      },
      {
        $sort: {
          sum: -1
        }
      }
    ]);
    // await result.forEach(doc => console.log(doc));
  } catch (error) {
    console.log(error);
  }
}

//- Calculate the average score for homework for all students
async function example13() {
  try {
    const result = await studentsCollection.aggregate([
      {$unwind: '$scores'},
      {
        $match: {
          'scores.type': 'homework'
        }
      },
      {$group: {_id: null, avgForHome: {$avg: '$scores.score'}}}
    ]);
    // await result.forEach(doc => console.log(doc));
  } catch (error) {
    console.log(error);
  }
}

// Delete all students that have homework score <= 60
async function example14() {
  try {
    const {result} = await studentsCollection.deleteMany({
      scores: {
        $elemMatch: {
          type: 'homework',
          score: {$lt: 60}
        }
      }
    });
    console.log(`Deleted ${result.n} articles with results lower than 60`);
  } catch (error) {
    console.log(error);
  }
}

//Mark students that have quiz score => 80
async function example15() {
  try {
    const {result} = await studentsCollection.updateMany(
      {
        scores: {
          $elemMatch: {
            type: 'quiz',
            score: {$gt: 80}
          }
        }
      },

      {$set: {status: 'marked'}}
    );
    console.log(`Updated ${result.n} students as marked`);
  } catch (error) {
    console.log(error);
  }
}

//Write a query that groups students by 3 categories (calculate the average grade for three subjects)
// - a => (between 0 and 40)
// - b => (between 40 and 60)
// - c => (between 60 and 100)

async function example16() {
  try {
    const result = await studentsCollection.aggregate([
      {$unwind: '$scores'},
      //finds average grade for 3 subjects
      {$group: {_id: {id: '$name'}, avg: {$avg: '$scores.score'}}}
    ]);
    // await result.forEach(doc => console.log(doc));
  } catch (error) {
    console.log(error);
  }
}
