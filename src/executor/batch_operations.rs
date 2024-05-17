use crate::prelude::*;
use crate::IntoActiveModel;
use std::str::FromStr;

#[derive(Debug)]
pub struct BatchOperations<Model>
where
    Model: ActiveModelTrait + ActiveModelBehavior + Send + Sync,
    <Model::Entity as EntityTrait>::Model: IntoActiveModel<Model>,
{
    primary_key_column_name: String,
    create: Vec<Model>,
    update: Vec<Model>,
    delete: Vec<<<Model::Entity as EntityTrait>::PrimaryKey as PrimaryKeyTrait>::ValueType>,
}

impl<Model> BatchOperations<Model>
where
    Model: ActiveModelTrait + ActiveModelBehavior + Send + Sync,
    <Model::Entity as EntityTrait>::Model: IntoActiveModel<Model>,
    <<Model::Entity as EntityTrait>::PrimaryKey as PrimaryKeyTrait>::ValueType: Clone + Into<Value>,
{
    #[must_use]
    pub fn new(primary_key_column_name: String) -> Self {
        Self {
            primary_key_column_name,
            create: vec![],
            update: vec![],
            delete: vec![],
        }
    }

    pub fn add_create_operation(&mut self, model: Model) -> &mut Self {
        self.create.push(model);
        self
    }

    pub fn add_update_operation(&mut self, model: Model) -> &mut Self {
        self.update.push(model);
        self
    }

    pub fn add_delete_operation(
        &mut self,
        id: <<Model::Entity as EntityTrait>::PrimaryKey as PrimaryKeyTrait>::ValueType,
    ) -> &mut Self {
        self.delete.push(id);
        self
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.create.len() + self.update.len() + self.delete.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn primary_key_column(&self) -> Result<<Model::Entity as EntityTrait>::Column, DbErr> {
        <Model::Entity as EntityTrait>::Column::from_str(self.primary_key_column_name.as_str())
            .map_err(|_| {
                DbErr::Type("Invalid primary key column name provided: doesn't exist".to_string())
            })
    }

    pub async fn process(self, db: &DatabaseConnection) -> Result<u64, DbErr> {
        let result = self.len() as u64;
        let primary_key_column = self.primary_key_column()?;

        let Self {
            create,
            update,
            delete,
            ..
        } = self;

        for chunk in create.chunks(1000) {
            Model::Entity::insert_many(chunk.to_vec()).exec(db).await?;
        }

        for item in update {
            Model::save(item, db).await?;
        }

        if !delete.is_empty() {
            for items in delete.chunks(1000) {
                Model::Entity::delete_many()
                    .filter(primary_key_column.is_in(items.iter().cloned().map(Into::into)))
                    .exec(db)
                    .await?;
            }
        }

        Ok(result)
    }
}
