use serde::Deserialize;

#[derive(Deserialize)]
struct StateUpdate {
    state_diff: StateDiff,
}

#[derive(Deserialize)]
struct StateDiff {
    deployed_contracts: Vec<Contract>,
    declared_classes: Vec<Class>,
}

#[derive(Deserialize)]
struct Contract {
    class_hash: String,
}

#[derive(Deserialize)]
struct Class {
    class_hash: String,
}

pub fn extract_class_hash(srd_state_update: &str) -> Result<Vec<String>, String> {
    let state_update: StateUpdate =
        serde_json::from_str(srd_state_update).map_err(|e| e.to_string())?;

    let state_diff = state_update.state_diff;

    let mut class_hashes = vec![];

    state_diff.deployed_contracts.iter().for_each(|contract| {
        class_hashes.push(contract.class_hash.clone());
    });
    state_diff.declared_classes.iter().for_each(|class| {
        class_hashes.push(class.class_hash.clone());
    });

    Ok(class_hashes)
}
